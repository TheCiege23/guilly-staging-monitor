# Guilly staging monitor

Private GitHub-hosted Ubuntu runner; hourly at minute 17 and manual dispatch. Scheduling is best effort. No local app server is needed. Only the four embedded QA leagues and guarded staging database are processed. Durable staging Postgres checkpoints preserve progress across runs.

Manual test_alerts sends labeled test emails without marking any sport complete. Actual completion requires live cuts, double chops, survivor waivers and one winner for all four sports. Workflow failure fallback sends email when the runner is operational; platform outages or disabled workflows cannot self-notify.
