Lakh
==========

Background job processing server in Rust loosely inspired by [faktory](https://github.com/contribsys/faktory), [sidekiq](https://github.com/mperham/sidekiq) and similar.

Features
------------

- immediate/scheduled/delayed jobs
- automatic job retry with exponential backoff
- job reservation (retry if status confirmation doesn't arrive within reservation time)

API
------------

Lakh uses gRPC as its communication layer so that clients and workers can be implemented in any language without much friction. Proto definition is avalible [here](https://github.com/HichuYamichu/lakh/blob/master/src/proto/workplace.proto). Clients and workers are expected to send metadata entry named `job_names` with semicolon separated list of job names this worker/client is offering to do/wants someone to do. Not doing so will result in ignoring this worker/client. Example client and worker implementations are available [here](https://github.com/HichuYamichu/lakh/tree/master/src/producer) and [here](https://github.com/HichuYamichu/lakh/tree/master/src/consumer).

Notes
------------

-  if job has no reservation time it is assumed it succeeds immediately after being sent and future status reports about it are ignored
-  failed job is automatically retried up to 30 times, after which it's considered dead and won't be attempted anymore
- worker unavailability doesn't count as job failure 
- job is considered failed after receiving negative status report or after reservation time elapses and no status report was received during this time
- only first status report about particular job is considered all subsequent reports are ignored. This means in case of a job being sent to many workers beacuse of (possibly numerous) reservation expirations we respect the first report we get regardless of wheter the worker reporting is the original one, latest one or any other that happend to receive this job.
- if there are no available workers to do particular job, all incoming jobs will have to wait and once required worker arrives all waiting jobs will be sent to it (therefore streaming large amounts of jobs while no worker is present is not recommended)

TODO
------------

- logging
- configuration
- introspection API
- persistence?
