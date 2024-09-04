use crate::error::Result;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::UnboundedReceiver,
    time::{interval, sleep, timeout},
};
use worterbuch_client::{
    topic, TypedStateEvent, TypedStateEvents,
    Worterbuch,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vote {
    instance_id: Box<str>,
    term: u64,
}

enum State {
    Leader,
    Follower,
    Candidate,
    Stopped,
}

#[derive(Debug, Default)]
struct Election {
    timed_out: bool,
    votes_total: usize,
    votes_in_my_favor: usize,
    new_leader: Option<Box<str>>,
}

struct LeaderElection<S: FnMut(), F: Future + Unpin> {
    // constants
    wb: Worterbuch,
    group_name: Box<str>,
    request_shutdown: S,
    on_shutdown_requested: F,
    instance_id: Box<str>,
    quorum: usize,
    root_key: Box<str>,
    election_key: Box<str>,
    min_delay: u64,
    max_delay: u64,
    heartbeat: u64,

    // mutable state
    state: State,
    leader_id: Option<Box<str>>,
    term: u64,

    // subscriptions
    votes: UnboundedReceiver<Option<Vote>>,
    heartbeats: UnboundedReceiver<TypedStateEvents<usize>>,
}

impl<S: FnMut(), F: Future + Unpin> LeaderElection<S, F> {
    async fn new(
        namespace: Option<Box<str>>,
        wb: Worterbuch,
        instance_id: Box<str>,
        group_name: Box<str>,
        quorum: usize,
        min_delay: u64,
        max_delay: u64,
        heartbeat: u64,
        request_shutdown: S,
        on_shutdown_requested: F,
    ) -> Result<Self> {
        let root_key:Box<str> = format!(
            "{}/{}",
            namespace.unwrap_or("leader/election".into()),
            group_name
        ).into();
        let election_key: Box<str> = format!("{}/{}", root_key, instance_id).into();

        let (votes, _) = wb.subscribe::<Vote>(topic!(root_key), true, true).await?;

        let (heartbeats, _) = wb
            .psubscribe::<usize>(topic!(root_key, "?"), false, false, None)
            .await?;

        Ok(Self {
            wb,
            group_name,
            request_shutdown,
            on_shutdown_requested,
            instance_id,
            quorum,
            root_key,
            election_key,
            state: State::Candidate,
            leader_id: None,
            min_delay,
            max_delay,
            heartbeat,
            term: 0,
            votes,
            heartbeats,
        })
    }

    async fn start(mut self) -> Result<()> {
        log::info!(
            "Participating in leader election for group '{}' as instance '{}' …",
            self.group_name,
            self.instance_id
        );

        log::debug!("namespace:\t{}", self.root_key);
        log::debug!("min delay:\t{}", self.min_delay);
        log::debug!("max delay:\t{}", self.max_delay);
        log::debug!("heartbeat:\t{}", self.heartbeat);

        self.run().await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            match &self.state {
                State::Leader => self.lead().await?,
                State::Follower => self.follow().await?,
                State::Candidate => self.start_election().await?,
                State::Stopped => {
                    (self.request_shutdown)();
                    break;
                },
            }
        }

        Ok(())
    }

    async fn start_election(&mut self) -> Result<()> {
        log::debug!("Subscribed to votes.");

        log::debug!("Starting election …");

        self.leader_id = None;

        let mut election = Election::default();

        let delay = thread_rng().gen_range(self.min_delay..=self.max_delay);

        log::debug!("Delay before voting for myself: {}ms", delay);

        let mut voted = false;

        if timeout(Duration::from_millis((self.heartbeat * 2) as u64), async {
            'election: loop {
                select! {
                    _ = &mut self.on_shutdown_requested => {
                        self.state = State::Stopped;
                        break 'election;
                    },
                    _ = sleep(Duration::from_millis(delay as u64)) => {
                        if!voted {
                            voted = true;
                            self.term += 1;
                            log::debug!("Voting for myself.");
                            if let Err(e) = self.wb
                                .publish(
                                    topic!(self.root_key),
                                    &Vote {
                                        instance_id: self.instance_id.clone(),
                                        term: self.term,
                                    },
                                ).await {
                                log::error!("Failed to vote for myself: {e}");
                                break 'election;
                            }
                        }
                    },
                    recv = self.votes.recv() => if let Some(Some(vote)) = recv {
                        match self.count_vote(&mut election, vote).await {
                            Ok(ControlFlow::Break(_)) => break 'election,
                            Ok(_) => (),
                            Err(e) => {
                                log::error!("Error processing vote: {e}");
                                break 'election;
                            },
                        }
                    } else {
                        break 'election; 
                    },
                    recv = self.heartbeats.recv() => match recv {
                        Some(es) => {
                            for e in es {
                                match e {
                                    TypedStateEvent::KeyValue(kvp) => {
                                        if let Some(current_leader) = kvp.key.split('/').skip(2).next() {
                                            log::info!("Found existing leader {current_leader}, following it.");
                                            election.new_leader = Some(current_leader.into());
                                            break 'election;
                                        }
                                    },
                                    TypedStateEvent::Deleted(kvp) => {
                                        if let Some(leader) = &self.leader_id {
                                            let leader: &str = leader.as_ref();
                                            if kvp.key.ends_with(leader) {
                                                log::info!("The current leader died.");
                                                if leader == self.instance_id.as_ref() {
                                                    log::info!("Apparently that was me. Bye.");
                                                    (self.request_shutdown)();
                                                }else {
                                                    self.state = State::Candidate;
                                                    break 'election;
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                        },
                        None => break 'election,
                    },
                }
            }
        })
        .await
        .is_err()
        {
            log::warn!("Election timed out.");
            election.timed_out = true;
        }

        log::debug!("election done: {election:?}");

        if election.timed_out {
            log::info!("Starting new election term …");
            self.leader_id = None;
            self.state = State::Candidate;
        } else if election.votes_in_my_favor >= self.quorum {
            log::info!("We won the election.");
            self.leader_id = Some(self.instance_id.clone());
            self.state = State::Leader;
        } else if let Some(leader) = election.new_leader {
            log::info!("Instance '{leader}' won the election.");
            self.leader_id = Some(leader);
            self.state = State::Follower;
        } else {
            log::info!("Election was tied. Starting new term …");
            self.leader_id = None;
            self.state = State::Candidate;
        }

        Ok(())
    }

    async fn count_vote(&self, election: &mut Election, vote: Vote) -> Result<ControlFlow<(), ()>> {
        log::debug!("Received vote: {vote:?}");

        if vote.term < self.term {
            log::debug!("Ignoring vote for old term {}.", vote.term)
        } else if vote.term == self.term {
            election.votes_total += 1;
            if self.instance_id == vote.instance_id {
                log::debug!("Received vote for me.");
                election.votes_in_my_favor += 1;
                if election.votes_in_my_favor >= self.quorum {
                    return Ok(ControlFlow::Break(()));
                }
            } else {
                log::debug!(
                    "Received vote for candidate {}, ignoring.",
                    vote.instance_id
                );
            }
            if election.votes_total > 2 * self.quorum {
                return Ok(ControlFlow::Break(()));
            }
        } else if vote.term > self.term {
            log::debug!(
                "Received vote for candidate {} with higher term {}, supporting candidate.",
                vote.instance_id,
                vote.term
            );
            self.wb.publish(topic!(self.root_key), &vote).await?;
            election.new_leader = Some(vote.instance_id);
            return Ok(ControlFlow::Break(()));
        }

        Ok(ControlFlow::Continue(()))
    }

    async fn follow(&mut self) -> Result<()> {

        'follow: loop {
            select! {
                _ = &mut self.on_shutdown_requested => {
                    self.state = State::Stopped;
                    break 'follow;
                },
                _ = sleep(Duration::from_millis((2 * self.heartbeat) as u64)) => {
                    log::info!("Did not receive heartbeat from leader, starting new election …");
                    self.state = State::Candidate;
                    break 'follow;
                },
                _ = self.votes.recv() => (),
                recv = self.heartbeats.recv() => match recv {
                    Some(es) => {
                        for e in es {
                            match e {
                                TypedStateEvent::Deleted(kvp) => {
                                    if let Some(leader) = &self.leader_id {
                                        let leader: &str = leader.as_ref();
                                        if kvp.key.ends_with(leader) {
                                            log::info!("The current leader died.");
                                            self.state = State::Candidate;
                                            break 'follow;
                                        }
                                    }
                                },
                                _ => (),
                            }
                        }
                    },
                    None => break 'follow,
                },
            }
        }

        Ok(())
    }

    async fn lead(&mut self) -> Result<()> {
        self.wb
            .set_grave_goods(&[&topic!(self.election_key)])
            .await?;
        self.wb.set(topic!(self.election_key), self.term).await?;

        let mut interval = interval(Duration::from_millis(self.heartbeat));

        'lead: loop {
            select! {
                _ = &mut self.on_shutdown_requested => {
                    self.state = State::Stopped;
                    break 'lead;
                },
                _ = interval.tick() => {
                    self.wb.set(topic!(self.election_key), self.term).await?;
                }
                _ = self.votes.recv() => (),
                recv = self.heartbeats.recv() => match recv {
                    Some(es) => {
                        for e in es {
                            match e {
                                TypedStateEvent::Deleted(kvp) => {
                                    if let Some(leader) = &self.leader_id {
                                        let leader: &str = leader.as_ref();
                                        if kvp.key.ends_with(leader) {
                                            log::info!("The current leader died.");
                                            if leader == self.instance_id.as_ref() {
                                                log::info!("Apparently that was me. Bye.");
                                                self.state = State::Stopped;
                                                return Ok(());
                                            }
                                        }
                                    }
                                },
                                _ => (),
                            }
                        }
                    },
                    None => break 'lead,
                },
            }
        }

        Ok(())
    }
}

pub async fn elect_leader<'a, S: FnMut(), F: Future + Unpin + 'a>(
    namespace: Option<Box<str>>,
    wb: Worterbuch,
    instance_id: Box<str>,
    group_name: Box<str>,
    quorum: usize,
    min_delay: u64,
    max_delay: u64,
    heartbeat: u64,
    request_shutdown: S,
    on_shutdown_requested: F,
) -> Result<()> {
    let le = LeaderElection::new(
        namespace,
        wb,
        instance_id,
        group_name,
        quorum,
        min_delay,
        max_delay,
        heartbeat,
        request_shutdown,
        on_shutdown_requested,
    )
    .await?;

    le.start().await
}
