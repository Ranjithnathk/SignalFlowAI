# SignalFlow AI
### E-commerce Operational Intelligence from Customer Signals

### Business Problem
- E-commerce platforms receive millions of customer complaints and low-rated reviews.
- Teams fix individual issues reactively, but recurring systemic failures go unnoticed.
- This leads to repeated refunds, damaged brand reputation, and increased churn.
- Not a reactive ticket handling, it is a proactive operational decision intelligence platform.

## Week 1 (01/31/2026)
**Problem Statement:** Today, supply chains face frequent disruptions due to weather, port congestion, supplier delays, and geopolitical issues. Even though this information exists across different systems and news sources, it is scattered and analyzed too late, which causes delivery delays, operational downtime, and revenue loss.

**Proposed Solution:** We planned to bring this data together into a single system and uses AI to detect early warning signals of potential disruptions. It explains why a risk is increasing and recommends actions, helping businesses plan ahead and reduce losses instead of reacting after problems occur.

**Feedback from Professor:** The problem statement is good and the solution is very narrow. Chances of having relevant data would be less.

--

## Week 2 (02/07/2026)
We realized that the core problem we want to solve is not predicting risks, but helping organizations learn from real operational signals and make better preventive decisions. So we reworked on our business problem and project focus.

Updated idea: **SignalFlow AI** is a decision-intelligence system that learns from customer-reported issues such as support tickets and complaints-to identify recurring operational problems and help teams fix root causes before issues repeat.
This project solves a real business problem faced by large platforms, repeated customer issues that are handled reactively but uses publicly available, text-rich datasets. It allows us to demonstrate data engineering, agentic reasoning, and RAG without relying on hard-to-access data sources.

--

## Week 3 (02/14/2026)
We discussed and organized the project flow,
- Data Engineering phase
- AI Layer phase
- Agentic Intelligence phase
- MCP Orchestration phase
- UI phase

**Dataset:** Initially we had an idea of taking real world data, which is Consumer Financial Protection Bureau (CFPB) Dataset from www.catalog.data.gov. But that data is particularly focusing on complaints raised on consumer financial products and services. Our focus was to resolve serious business problem in a top organization like Amazon, so we arrived at choosing Amazon Reviews 2018 dataset.
This dataset was widely used, but our goal was to shift from usual reactive ticket handling systems to build proactive operational decision intelligence.
Out of many categories, we limited our focus into **Electronic** and **Home & Kitchen** categories where there are 43Million reviews and 2Million products (metadata).

--

## Week 4 (02/21/2026)
We focused on Data Engineering phase.

#### Repository & Git Setup
- Created GitHub repository
- Added basic project skeleton structure
- Enabled collaborators for everyone in the team

#### UCSD to S3 Ingestion
- AWS S3 bucket setup, IAM (role, policy) setup
- Implemented data ingestion from UCSD dataset (web) to AWS S3 using python scripts
- Ensure Reviews and Metadata are stored in proper folder structure and dated format on AWS S3
- Transform to Parquet and stored into landing/parquet/

#### Snowflake setup
- Created Warehouse, Database, Schema setup 
- Created Storage Integration from S3
- Created RAW tables for Reviews and Metadata, COPY INTO, and Validation Queries
- Verified the counts of records
- Total 4 SQL files on Snowflake

#### dbt (in progress)

#### Airflow DAG Orchestration (To do)

#### DEV vs PROD base parameterization