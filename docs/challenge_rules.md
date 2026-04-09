# Nedbank Data & Analytics Challenge
## Data Engineering Track — Challenge Rules

**Document type:** Participant-Facing Rules Document
**Prepared by:** Otinga
**Date:** 2 April 2026
**Status:** DRAFT — Pending Legal Review
**Classification:** External (Participant-Facing upon publication)

> **NOTE TO REVIEWERS:** This document contains multiple [LEGAL REVIEW REQUIRED] and [TBC] markers. These must be resolved before the document is published to participants. Do not distribute this draft externally.

---

## 1. Eligibility

### 1.1 Residency

The Nedbank Data & Analytics Challenge (the "Challenge") is open to individuals who are South African residents at the time of registration.

**[LEGAL REVIEW REQUIRED — age requirement]:** A minimum age requirement must be confirmed. Recommended options are 18 years (standard for prize competitions and contractual capacity) or the statutory school-leaving age. Until confirmed, all registered participants must be 18 or older.

### 1.2 Individual Participation

The Challenge is structured for individual participation only. Team submissions are not permitted. Each registered participant must be a single natural person. Accounts may not be shared.

### 1.3 Nedbank Employees

**[LEGAL REVIEW REQUIRED — employee eligibility]:** It must be confirmed whether Nedbank employees (including contractors, secondees, and employees of Nedbank subsidiary entities) are eligible to participate, and if so, whether they are eligible to receive prizes. Otinga employees directly involved in challenge design are excluded from participation.

Until this is confirmed, the following placeholder applies:

> Employees of Nedbank Group Limited and its subsidiaries are **[eligible to participate but not eligible to receive prizes / excluded from participation — CONFIRM]**. Employees of Otinga (Pty) Ltd who have had access to challenge design materials are excluded from participation.

### 1.4 One Submission Per Participant Per Stage

Each participant may make only one scored submission per stage. Only the submission tagged with the correct Git tag (see Section 3) at the time the stage window closes will be evaluated. Participants may push multiple commits to their repository during the stage window, but only the tagged commit counts.

### 1.5 Disqualification at Registration

Participants found to have provided false eligibility information at registration will be disqualified without notice and any prizes forfeited.

---

## 2. Timeline

All times are South Africa Standard Time (SAST, UTC+2).

| Event | Timing |
|---|---|
| Challenge opens | Day 1, 00:00 SAST |
| Stage 1 submission closes | Day 7, 23:59 SAST |
| Stage 2 specification released | Day 8, 00:00 SAST |
| Stage 1 questionnaire window | Within 2 hours of Stage 1 close |
| Stage 2 submission closes | Day 14, 23:59 SAST |
| Stage 3 specification released | Day 15, 00:00 SAST |
| Stage 2 questionnaire window | Within 2 hours of Stage 2 close |
| Stage 3 (final) submission closes | Day 21, 23:59 SAST |
| Stage 3 questionnaire window | Within 2 hours of Stage 3 close |
| Results announced | Within 2 weeks of Stage 3 close |
| On-site finalist event | **[DATE TBC — confirm with Nedbank]** |

### 2.1 Technical Questionnaires

Within 2 hours of each stage submission window closing, qualifying participants will receive a link to a timed technical questionnaire. Participants must complete the questionnaire within the time limit from the moment the link is opened:

- Stage 1 questionnaire: 30-minute completion window
- Stage 2 questionnaire: **[TBC]** minutes
- Stage 3 questionnaire: **[TBC]** minutes

Failure to complete a questionnaire does not disqualify a participant from the automated scoring phase, but questionnaire responses are used in the on-site evaluation of finalists (see Section 6.3).

### 2.2 Results Notification

Finalists will be notified by email to the address used at registration within 2 weeks of the Stage 3 submission close. If a finalist does not respond to the notification within **[TBC]** business days, their place in the on-site event may be forfeited.

---

## 3. Submission Rules

### 3.1 Submission Method

Participants submit their work by entering a Git repository URL on the Otinga challenge platform before the applicable stage deadline. The scoring system will clone the repository, build the Docker image, and execute the pipeline against the evaluation dataset. Participants do not need to be present during scoring runs.

### 3.2 Git Tags

Submissions are identified by Git tags applied to the relevant commit. The required tags are:

| Stage | Required Git Tag |
|---|---|
| Stage 1 | `stage1-submission` |
| Stage 2 | `stage2-submission` |
| Stage 3 | `stage3-submission` |

Only commits bearing the correct tag at the time the submission window closes will be scored. Tags that do not match exactly (e.g. `stage1`, `Stage1-submission`) will not be recognised by the scoring system.

### 3.3 Late Submissions

Submissions received after the stage deadline — including repository pushes and tag applications made after the window closes — will not be accepted. No extensions will be granted except in the event of a documented platform outage affecting the submission system, at Otinga's sole discretion.

### 3.4 Repository Access

Your repository must be publicly accessible, or read access must be granted to the Otinga scoring account (details provided on the platform) before the submission deadline. Repositories that are private and inaccessible to the scoring account at the time of scoring will be treated as non-submissions.

### 3.5 Immutability of Tagged Commits

Once a stage submission window has closed, participants must not modify or re-tag the relevant stage commit. Amending, force-pushing, or re-tagging a submitted commit after the deadline will result in disqualification.

### 3.6 Required Repository Structure

Your repository must contain the following structure. The scoring system requires these entry points to be present:

```
Dockerfile                  # Must extend the provided base image
pipeline/
  ingest.py                 # Bronze layer ingestion
  transform.py              # Bronze → Silver transformation
  provision.py              # Silver → Gold provisioning
  stream_ingest.py          # Stage 3: streaming ingestion
config/
  pipeline_config.yaml      # Pipeline configuration (paths, thresholds)
  dq_rules.yaml             # Data quality rules (Stage 2 onwards)
adr/
  stage3_adr.md             # Stage 3: Architecture Decision Record
requirements.txt
README.md
```

Additional modules under `pipeline/` are permitted. The four named pipeline entry points are required from Stage 1 (stub implementations are acceptable in Stage 1 for Stage 2+ files).

---

## 4. Technical Rules

### 4.1 Execution Environment

All pipeline code must execute inside a Docker container built on the provided base image. The scoring system mounts input data and executes your container using the following invocation:

```bash
docker run \
  -v /path/to/data:/data \
  -m 2g --cpus="2" \
  candidate-submission:latest
```

Your Dockerfile must extend the provided base image. Your container must:
- Exit with code `0` on successful completion
- Exit with a non-zero code on failure
- Not require interactive input at runtime

### 4.2 Resource Limits

The following resource constraints are enforced by Docker and cannot be overridden:

| Resource | Limit |
|---|---|
| RAM | 2 GB |
| CPU | 2 vCPU |
| Execution time (per stage) | 15 minutes (Stage 1) / 30 minutes (Stages 2 and 3) |

> **Note:** The execution time limit for Stage 1 is 15 minutes. Stage 2 and Stage 3 operate under a 30-minute limit to accommodate the increased data volume. Pipelines that exceed the time limit will be terminated and scored as incomplete on the Correctness and Scalability dimensions.

### 4.3 Network Access

Your pipeline must not depend on external network access at runtime. The execution environment has no internet connectivity. Any libraries or dependencies must be declared in `requirements.txt` and installed during the Docker build phase.

Azure ADLS Gen 2 storage is available as an optional output target. Participants wishing to use Azure storage will be provided credentials via the platform. Use of Azure is not required and provides no scoring advantage.

### 4.4 Permitted Languages and Frameworks

There are no restrictions on programming language, library, or data processing framework, provided the chosen tools can be installed within the Docker container during the build phase. The base image includes:

| Component | Version |
|---|---|
| Python | 3.11 |
| PySpark | 3.5 |
| delta-spark | Pre-installed |

Participants are free to use Python, Scala, SQL, R, or any combination. Orchestration approach (single script, DAGs, notebooks) is unconstrained.

### 4.5 Excluded Technologies

The following technologies are explicitly excluded from the competition environment and may not be used:

| Technology | Reason for Exclusion |
|---|---|
| Ab Initio | Licensed; not accessible to external participants |
| Microsoft Fabric / OneLake | Licensing constraints for external participants |
| Azure Data Factory (ADF) | GUI-based; not assessable in a code submission |
| Kubernetes | Infrastructure concern, not a candidate deliverable |

Submissions that depend on excluded technologies will not execute in the scoring environment and will receive a zero score on all automated dimensions.

---

## 5. AI Usage Policy

You may use any tools, including AI assistants, coding copilots, and generative AI systems.

Your submitted code must run correctly within the resource constraints. You must be able to explain and defend every design decision during the timed technical questionnaire after each stage and in front of the evaluation panel at the on-site finale.

**AI-generated code that you cannot explain is a liability, not an asset.** The questionnaire and on-site interrogation are specifically designed to distinguish candidates who built and understand their pipeline from candidates who submitted one. Evaluation is of the engineer, not the artefact.

Characteristic patterns in LLM-generated code — including use of pandas DataFrames at scale where native Spark operations apply, calling `.collect()` on large distributed datasets, looping over DataFrames instead of using Spark transformations, and redundant full-dataset re-reads — are detected by the automated scoring system and reduce the Efficiency score.

---

## 6. Scoring

### 6.1 Automated Scoring Dimensions

Submissions are scored programmatically across four dimensions:

| Dimension | Weight | What Is Measured |
|---|---|---|
| **Correctness** | 40% | Do outputs match expected results? Do validation queries pass? Does the DQ report reflect actual issue counts? Does the streaming Gold layer update within the 5-minute SLA (Stage 3)? |
| **Scalability** | 25% | Does the pipeline complete Stage 2 within the 30-minute time limit? Does it stay within memory limits? How does execution time compare across the cohort? |
| **Maintainability** | 20% | Is configuration externalised? Are modules separable? How many lines changed between Stage 1 and Stage 2 relative to total codebase? Does the Stage 3 ADR address all three required questions? |
| **Efficiency** | 15% | Is Spark used idiomatically? Peak memory below 80% of allocation? Any redundant full-dataset scans? |

Scores are normalised across the cohort where relevant. The final score for each stage is a weighted sum across the four dimensions, expressed as a value out of 100.

### 6.2 Advancement Threshold

The minimum score required to qualify for the on-site finale is **50 points** (out of 100). The **top 25 scorers** above this threshold advance to the finale event.

In the event that fewer than 25 participants achieve the minimum threshold, the on-site cohort will be limited to those who did.

### 6.3 Tie-Breaking

Where two or more participants have equal total scores, the following tie-breaking sequence applies:

1. Higher score on the Correctness dimension
2. Faster Stage 2 pipeline execution time
3. Earlier Stage 1 submission timestamp

### 6.4 On-Site Evaluation

The on-site finale is a technical interrogation, not a presentation. Each finalist will be evaluated in a 20-minute session with a panel comprising Nedbank senior data engineers (who designed the challenge) and an Otinga lead.

The panel will have read each finalist's code and questionnaire responses before the event. The interrogation will focus on specific implementation decisions, trade-offs made between stages, and responses to hypothetical changes. Questionnaire responses form part of the evidence base for this conversation.

On-site evaluation does not modify the automated score. Prize rankings are determined by the automated score. The on-site session is part of the hiring fast-track assessment process.

---

## 7. Intellectual Property

**[LEGAL REVIEW REQUIRED — full section requires legal input before publication]**

The following positions are proposed and must be reviewed and confirmed by legal counsel before this document is published:

7.1 **Participant IP:** Participants retain intellectual property rights in the code and artefacts they submit. Submission of an entry does not constitute an assignment of IP to Nedbank or Otinga.

7.2 **Licence for Scoring:** By submitting an entry, participants grant Nedbank and Otinga a non-exclusive, royalty-free licence to access, clone, execute, and evaluate the submitted code solely for the purposes of scoring and assessment within the Challenge. This licence does not extend to commercial use.

7.3 **No Sharing Between Participants:** Submitted code will not be shared with other participants at any point during or after the Challenge.

7.4 **Retention:** **[LEGAL REVIEW REQUIRED — confirm retention period and deletion obligations]** Otinga will retain cloned repository copies for a period of **[TBC]** after the challenge closes for audit and dispute resolution purposes, after which they will be deleted.

7.5 **Third-Party Code:** Participants are responsible for ensuring their submissions comply with the licences of any third-party libraries or frameworks used. Submissions that incorporate code in violation of a third-party licence are the sole responsibility of the participant.

---

## 8. Prizes

**[CONFIRM WITH NEDBANK before publishing]**

8.1 **Total Prize Pool:** The combined prize pool across the Data Engineering and Machine Learning tracks is **R500,000** (five hundred thousand rand).

8.2 **DE Track Prize Split:** The prize allocation for the Data Engineering track is:

| Place | Prize Amount |
|---|---|
| 1st Place | **[TBC]** |
| 2nd Place | **[TBC]** |
| 3rd Place | **[TBC]** |

Track-level splits will be confirmed and published before the challenge opens.

8.3 **Payment:** Prizes will be paid within **[TBC]** business days of the on-site finale via electronic funds transfer to a South African bank account in the winner's name. Winners will be required to provide banking details and **[LEGAL REVIEW REQUIRED — confirm SARS / FICA documentation requirements]** relevant tax and identity documentation before payment is processed.

8.4 **Prize Conditions:** Prizes are non-transferable and may not be exchanged for cash equivalents beyond the stated amount. **[LEGAL REVIEW REQUIRED — confirm whether prizes are subject to income tax / withholding obligations and who bears that cost]**

8.5 **Forfeiture:** If a prize winner cannot be contacted within **[TBC]** business days of notification, or declines to provide required documentation, the prize may be forfeited and reallocated at Nedbank and Otinga's discretion.

---

## 9. Code of Conduct

All participants are expected to engage with the Challenge in good faith. The following conduct is grounds for immediate disqualification:

9.1 **Plagiarism:** Submitting code that was written by another person (excluding AI tools, which are permitted under Section 5) without attribution, or substantially reproducing another participant's work.

9.2 **Collusion:** Sharing solutions, code, or implementation strategies with other participants during the competition window. Participants may discuss general approaches and tools (as they would in a public forum) but may not share specific implementation code or pipeline designs.

9.3 **Unauthorised Access:** Any attempt to access, probe, or interfere with the scoring infrastructure, the Otinga platform, other participants' repositories, or any system associated with the Challenge.

9.4 **Post-Deadline Modification:** Modifying, amending, re-tagging, or force-pushing changes to a submitted commit after the submission deadline has passed (see Section 3.5).

9.5 **Misrepresentation:** Providing false information at registration, during the questionnaire, or at the on-site event.

Disqualification decisions are made by Otinga, in consultation with Nedbank where appropriate, and are final. Disqualified participants forfeit all prizes and are not eligible for the hiring fast-track.

---

## 10. Privacy and Data

**[LEGAL REVIEW REQUIRED — full section requires POPIA compliance review before publication]**

10.1 **Challenge Dataset:** The data provided to participants as part of the challenge (account records, transaction events, customer records) is **entirely synthetic**. It does not contain real personal information, real financial data, or data relating to any identifiable individual. Participants must nevertheless treat it as confidential and must not publish or distribute it outside the challenge context.

10.2 **Participant Personal Information:** By registering for the Challenge, participants consent to Otinga collecting and processing their personal information (name, email address, contact details, country of residence, and submission artefacts) for the purposes of administering the Challenge, determining eligibility, communicating results, and facilitating the hiring fast-track programme. This processing is conducted in accordance with **[LEGAL REVIEW REQUIRED — reference Otinga's POPIA-compliant privacy policy once confirmed]**.

10.3 **Submission Code as Personal Data:** Where submitted code contains information that could identify a participant (such as their name in comments or commit history), this is treated as personal information and handled accordingly.

10.4 **Sharing with Nedbank:** Finalist submission materials — including code, questionnaire responses, and automated score breakdowns — will be shared with Nedbank for the purposes of the on-site evaluation and the hiring fast-track programme. By advancing to the finalist stage, participants consent to this sharing.

10.5 **Retention and Deletion:** **[LEGAL REVIEW REQUIRED — confirm retention periods under POPIA and whether participants have deletion rights prior to the end of the retention period]**

10.6 **Data Subject Rights:** Participants have the right to request access to their personal information held by Otinga, to request correction of inaccurate information, and to lodge a complaint with the Information Regulator (South Africa) if they believe their rights under POPIA have been violated. Contact: **[TBC — Otinga Information Officer details]**

---

## 11. General Conditions

11.1 **Amendments:** Otinga reserves the right to amend these rules at any time prior to the challenge opening. Participants will be notified of material changes via the communication channel. After the challenge opens, rules will not be changed except where required by law or to correct a material error, and any such change will be communicated to all registered participants.

11.2 **Platform Availability:** Otinga will make reasonable efforts to ensure the submission platform is available during the competition window. In the event of a platform outage that materially affects participants' ability to submit, Otinga may, at its sole discretion, extend a submission deadline. Such extensions will be communicated to all participants and applied uniformly.

11.3 **Scoring Disputes:** Participants may raise a scoring dispute within **[TBC]** business days of receiving their score notification, by contacting the challenge support channel with specific details of the alleged error. Dispute decisions are made by Otinga and are final.

11.4 **Governing Law:** **[LEGAL REVIEW REQUIRED — confirm governing law and jurisdiction]** These rules are governed by the laws of the Republic of South Africa.

11.5 **Force Majeure:** Otinga and Nedbank are not liable for any failure to perform obligations under these rules due to circumstances beyond their reasonable control.

---

*For technical questions about the challenge environment and Docker setup, contact the challenge support channel. Problem-solving hints will not be provided.*

*For questions about these rules, eligibility, or prize administration, contact: **[TBC — challenge admin contact]***

---

**Document version:** 0.1 Draft
**Last updated:** 2 April 2026
**Next review:** Prior to publication
