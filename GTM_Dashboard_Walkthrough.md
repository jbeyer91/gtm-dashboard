# GTM Dashboard — Management Walkthrough Guide

## What this is

A live view of new business sales performance, pulled from HubSpot and refreshed hourly. It covers the full funnel from outbound activity through pipeline to closed revenue. Data is for the current month unless you change the period filter.

---

## Pages at a glance

| Page | What it answers |
|------|----------------|
| **Home** | Where are we vs quota this month? |
| **Scorecard** | How is each rep performing overall? |
| **Call Stats** | Are reps making enough calls? Are calls connecting? |
| **Deals Won** | What revenue has closed and where did it come from? |
| **Deal Advancement** | Of deals created, how many are actually moving? |
| **Pipeline Coverage** | What open pipeline do we have and how much is qualified? |
| **Pipeline Generated** | How much new pipeline is being created, by source? |
| **Book Coverage** | Are reps actively working their named accounts? |
| **ABM Coverage** | How is the target account list being covered? |
| **Inbound Funnel** | How are inbound leads converting by source? |
| **Forecast** | What has each rep submitted as their HubSpot forecast vs quota? |

---

## Page-by-page guide

### Home
The team-level pulse check. Start every review here.

- **Monthly Pace bar** — the white tick mark is where we *should* be given the business days elapsed. The fill is where we actually are. If the bar is behind the tick, we're behind pace.
- **Quota Attainment** — revenue closed this month as % of monthly quota. Target is 100%.
- **Deals Created** — new deals entered in CRM this month. Target is 13 per rep.
- **$ to Stage 2** — dollar value of deals that advanced to a qualified stage. A leading indicator of future closes.
- **Avg Dials / Day** — per-rep average outbound dials per working day. Target is 40.
- **Connect Rate** — % of dials that reached a live person. Target is 10%.

---

### Scorecard
Rep-by-rep composite grade. Use this to quickly see who needs attention.

**Grade is calculated from:**
- Quota attainment — 50%
- $ advanced to Stage 2 — 15%
- Deals created — 12%
- Stale accounts — 10%
- Avg dials/day — 8%
- Connect rate — 5%

**Reading the rep table:**
- The **Grade** column carries the composite signal — it's the main thing to look at
- **Quota %** is color-coded (green ≥ 80%, amber ≥ 60%, red below)
- All other columns are intentionally neutral gray — use them for context, not as independent red flags
- **Stale Accounts** turns red only above 40% — meaning more than 40% of a rep's A–C accounts have had no activity in 30 days

**Insight strip** at the top: quick snapshot of how many reps are at 80%+ and how many are below 50%.

---

### Call Stats
Outbound activity detail. Use the period filter to switch between Today, This Week, Last Week, and This Month.

**Key metrics:**
- **Total Dials** — all outbound call attempts
- **Connect Rate** — % that reached a live person (target: 10%)
- **Conversation Rate** — % of live connects that became a real conversation (1+ minute)
- **CO Deals Created** — cold outreach deals created directly from calls
- **Deals → S2** — cold outreach deals that advanced to Stage 2 or beyond

**In the rep table:**
- Connect % is color-coded: green ≥ 15%, yellow ≥ 10%, red below
- The **Avg / Day** total row shows the per-rep average, not a sum — this is the number to compare to the 40/day target
- Hover column headers for definitions

---

### Deal Advancement
Cohort view: of all deals *created* in a period, where have they gone?

This is a funnel, not a snapshot. If you select "This Month," you're seeing: of deals created this month, how many have progressed to each stage.

**Funnel visual** shows the progression rates at a glance — bars are % of the created cohort that reached each stage.

**Use this to answer:**
- Are deals stalling at Stage 1? (low S2 conversion)
- Are qualified deals converting to closes? (S3/S4 to Won)
- What's the loss rate from this cohort?

---

### Pipeline Coverage
Live snapshot of all open deals, bucketed by expected close date period and stage.

**Stage mix bar** shows the health of the pipeline: S1 is early/unqualified (gray), S2–S4 are progressively more qualified (blue to purple).

**A healthy pipeline** should skew toward S2–S4. Heavy S1 concentration means lots of early-stage risk.

**Two tables:**
- Deal count by stage per rep
- Pipeline value by stage per rep

---

### Pipeline Generated
How much new pipeline was created in a period, broken down by source (Cold Outreach, Inbound, Referral, Conference).

Use this to understand where pipeline is actually coming from and whether the mix is healthy.

---

### Book Coverage
Named account coverage — are reps actively working the accounts they own?

- **Activity (30d)** — % of A–C tier accounts with any logged call, email, meeting, or note in the last 30 days. Target: 60%+
- **% Contacted (120d)** — % of A–C accounts that received an outbound call in the last 120 days. Target: 75%+
- **Overdue Tasks** — HubSpot tasks past their due date. Should be 0.

---

### ABM Coverage
Focused on target accounts only (HubSpot `Target Account = true`). Mirrors Book Coverage but filtered to the ABM list, with deal creation and close data by month and quarter.

---

### Inbound Lead Conversion Funnel
How inbound leads convert by traffic source.

**Volume Funnel table columns:**
- **DQ'd** — leads flagged as unqualified (not a fit, duplicate, or unresponsive) — red when non-zero
- **PG $** — Pipeline Generated: total deal value created from these leads
- **ACV Won** — average contract value of deals that closed from these leads

**Conversion Rates table** shows DQ %, Follow-Up %, Deal Creation %, and Win Rate by source.

---

### Forecast
HubSpot forecast submissions vs quota. Only shows the current month.

The **Forecast Submission** column (indigo) is what each rep manually submitted in HubSpot's forecast tool. Compare to **Won $** for actuals and **Quota $** for the target. Reps who haven't submitted show "Not submitted" in italic.

---

## Suggested live walkthrough flow (15–20 min)

**1. Start at Home (2 min)**
> "This is our real-time view of the month. We're at X% to quota with N business days left. The tick mark shows where pace expects us to be — we're [ahead / on pace / behind]."

**2. Drill to Scorecard (5 min)**
> "Here's each rep's composite grade. The grade weights quota most heavily at 50%, but also factors in pipeline activity and account coverage. Let's look at who's on track and who needs a conversation."
- Call out reps at risk (below 50% quota)
- Note any rep with a high grade but low quota (good process, slow close)
- Note any rep with decent quota but low dials/stale accounts (closing existing pipeline but not building new)

**3. Call Stats for activity context (3 min)**
> "The scorecard grades activity, but here's the detail. Team average is X dials/day against a 40/day target. Connect rate is Y% — target is 10%."
- Use the period filter to compare this week vs last week
- Flag any rep significantly below team average

**4. Pipeline Coverage for forward-looking view (3 min)**
> "This shows our open pipeline. The bar at the top shows the stage mix — we want to see S2–S4 dominating, not S1."
- Note the ratio of qualified (S2+) to total open pipeline
- Compare pipeline value to quota gap if relevant

**5. Deal Advancement if time allows (2 min)**
> "Of the deals we've created this month, here's where they've gone. X% have moved to Stage 2, Y% are already closed or lost."

**6. Forecast to close (2 min)**
> "Here's what the team has submitted. Combined submission is $X vs quota of $Y."
- Flag any rep who hasn't submitted
- Note meaningful gaps between submission and current closed

---

## Notes for the presenter

- Data refreshes hourly from HubSpot. If you want fresh data right before the meeting, hit **Refresh data** at the bottom of the left sidebar.
- The sidebar expands on hover (desktop) — icons on the left, labels appear on hover.
- All tables have CSV export (small link above each table) if you want to pull data into slides.
- Hover over any column header with an underline cursor for a definition tooltip.
- The **Team** filter (home page) and team pill buttons (other pages) let you filter between Veterans, Rising, or All reps.
