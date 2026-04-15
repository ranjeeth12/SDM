-- ============================================================================
-- SDM Platform - SQL Server Database Setup
-- ============================================================================
-- Run this script on your local SQL Server to create all tables and seed data.
-- Database: SDM_Platform (or your preferred name)
-- Schema: sdm
-- ============================================================================

USE master;
GO

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'SDM_Platform')
    CREATE DATABASE SDM_Platform;
GO

USE SDM_Platform;
GO

-- Create schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sdm')
    EXEC('CREATE SCHEMA sdm');
GO


-- ============================================================================
-- LOOKUP TABLES — Governed reference data for synthetic generation
-- ============================================================================

-- First names (gender-tagged)
IF OBJECT_ID('sdm.lkp_first_names', 'U') IS NOT NULL DROP TABLE sdm.lkp_first_names;
CREATE TABLE sdm.lkp_first_names (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    name        NVARCHAR(50)    NOT NULL,
    gender      CHAR(1)         NOT NULL,  -- M or F
    created_at  DATETIME2       NOT NULL DEFAULT GETDATE()
);

-- Last names
IF OBJECT_ID('sdm.lkp_last_names', 'U') IS NOT NULL DROP TABLE sdm.lkp_last_names;
CREATE TABLE sdm.lkp_last_names (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    name        NVARCHAR(50)    NOT NULL,
    created_at  DATETIME2       NOT NULL DEFAULT GETDATE()
);

-- Street names
IF OBJECT_ID('sdm.lkp_street_names', 'U') IS NOT NULL DROP TABLE sdm.lkp_street_names;
CREATE TABLE sdm.lkp_street_names (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    street      NVARCHAR(50)    NOT NULL,
    type        NVARCHAR(10)    NOT NULL,  -- St, Ave, Dr, Blvd, etc.
    created_at  DATETIME2       NOT NULL DEFAULT GETDATE()
);

-- ZIP / City / State / County (CareSource service area)
IF OBJECT_ID('sdm.lkp_zip_city_state', 'U') IS NOT NULL DROP TABLE sdm.lkp_zip_city_state;
CREATE TABLE sdm.lkp_zip_city_state (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    zip         CHAR(5)         NOT NULL,
    city        NVARCHAR(50)    NOT NULL,
    state       CHAR(2)         NOT NULL,
    county      NVARCHAR(50)    NOT NULL,
    created_at  DATETIME2       NOT NULL DEFAULT GETDATE()
);

CREATE INDEX IX_lkp_zip_state ON sdm.lkp_zip_city_state (state);


-- ============================================================================
-- PATTERN METADATA — Discovered patterns, tags, and vocabulary
-- ============================================================================

-- Patterns (system-discovered)
IF OBJECT_ID('sdm.patterns', 'U') IS NOT NULL DROP TABLE sdm.patterns;
CREATE TABLE sdm.patterns (
    id                  INT IDENTITY(1,1) PRIMARY KEY,
    pattern_key         NVARCHAR(200)   NOT NULL UNIQUE,
    contextual_name     NVARCHAR(200)   NOT NULL,
    grgr_ck             NVARCHAR(500),          -- JSON array of group keys
    sgsg_ck             NVARCHAR(500),          -- JSON array of subgroup keys
    cspd_cat            NVARCHAR(500),          -- JSON array of plan categories
    lobd_id             NVARCHAR(500),          -- JSON array of LOB IDs
    grgr_name           NVARCHAR(200),
    sgsg_name           NVARCHAR(200),
    cspd_cat_desc       NVARCHAR(200),
    plds_desc           NVARCHAR(200),
    cluster_id          INT             NOT NULL,
    member_count        INT             NOT NULL,
    pct_of_pop          DECIMAL(8,6),
    silhouette          DECIMAL(8,6),
    profile_json        NVARCHAR(MAX),          -- Full profile data
    ai_summary          NVARCHAR(MAX),
    created_at          DATETIME2       NOT NULL DEFAULT GETDATE(),
    updated_at          DATETIME2       NOT NULL DEFAULT GETDATE()
);

CREATE INDEX IX_patterns_name ON sdm.patterns (contextual_name);

-- Pattern rules (user-saved pattern configurations)
IF OBJECT_ID('sdm.pattern_rules', 'U') IS NOT NULL DROP TABLE sdm.pattern_rules;
CREATE TABLE sdm.pattern_rules (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    pattern_id      INT             REFERENCES sdm.patterns(id),
    rule_name       NVARCHAR(200)   NOT NULL,
    rule_text       NVARCHAR(MAX)   NOT NULL,
    member_count    INT,
    created_by      NVARCHAR(100)   DEFAULT 'system',
    created_at      DATETIME2       NOT NULL DEFAULT GETDATE()
);

-- Pattern tags
IF OBJECT_ID('sdm.pattern_tags', 'U') IS NOT NULL DROP TABLE sdm.pattern_tags;
CREATE TABLE sdm.pattern_tags (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    pattern_id      INT             NOT NULL REFERENCES sdm.patterns(id),
    tag             NVARCHAR(100)   NOT NULL,
    tag_source      NVARCHAR(50)    NOT NULL DEFAULT 'auto',
    confidence      DECIMAL(5,4)    DEFAULT 1.0,
    confirmed_by    NVARCHAR(100),
    confirmed_at    DATETIME2,
    created_at      DATETIME2       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_pattern_tag UNIQUE (pattern_id, tag)
);

CREATE INDEX IX_pattern_tags_tag ON sdm.pattern_tags (tag);

-- Tag vocabulary (synonyms for keyword search)
IF OBJECT_ID('sdm.tag_vocabulary', 'U') IS NOT NULL DROP TABLE sdm.tag_vocabulary;
CREATE TABLE sdm.tag_vocabulary (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    canonical_tag   NVARCHAR(100)   NOT NULL,
    synonym         NVARCHAR(100)   NOT NULL,
    category        NVARCHAR(50),
    sme_confirmed   BIT             DEFAULT 0,
    confirmed_by    NVARCHAR(100),
    confirmed_at    DATETIME2,
    created_at      DATETIME2       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_tag_synonym UNIQUE (canonical_tag, synonym)
);

CREATE INDEX IX_tag_vocab_synonym ON sdm.tag_vocabulary (synonym);


-- ============================================================================
-- GENERATION — Rules, configuration, and audit log
-- ============================================================================

-- Generation rules (per-field configuration)
IF OBJECT_ID('sdm.generation_rules', 'U') IS NOT NULL DROP TABLE sdm.generation_rules;
CREATE TABLE sdm.generation_rules (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    field_name      NVARCHAR(100)   NOT NULL UNIQUE,
    field_label     NVARCHAR(200)   NOT NULL,
    field_category  NVARCHAR(50)    NOT NULL DEFAULT 'denorm',
    gen_method      NVARCHAR(50)    NOT NULL DEFAULT 'sequential',
    data_type       NVARCHAR(50)    NOT NULL DEFAULT 'alphanumeric',
    length          INT             DEFAULT 10,
    prefix          NVARCHAR(50)    DEFAULT '',
    postfix         NVARCHAR(50)    DEFAULT '',
    start_value     INT             DEFAULT 1,
    domain          NVARCHAR(200)   DEFAULT '',
    format_pattern  NVARCHAR(200)   DEFAULT '',
    lookup_source   NVARCHAR(200)   DEFAULT '',
    active          BIT             DEFAULT 1,
    updated_by      NVARCHAR(100)   DEFAULT 'system',
    updated_at      DATETIME2       NOT NULL DEFAULT GETDATE()
);

CREATE INDEX IX_gen_rules_field ON sdm.generation_rules (field_name);

-- Generation log (audit trail)
IF OBJECT_ID('sdm.generation_log', 'U') IS NOT NULL DROP TABLE sdm.generation_log;
CREATE TABLE sdm.generation_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    pattern_ids     NVARCHAR(MAX)   NOT NULL,   -- JSON array
    data_type       NVARCHAR(50)    NOT NULL,   -- members, claims, enrollments
    volume          INT             NOT NULL,
    output_path     NVARCHAR(500),
    weighting       NVARCHAR(MAX),              -- JSON
    requested_by    NVARCHAR(100)   DEFAULT 'user',
    created_at      DATETIME2       NOT NULL DEFAULT GETDATE()
);


-- ============================================================================
-- SOURCE DATA — Member denorm (current POC structure)
-- Replace with Config Denorm + direct source reads when redesigning
-- ============================================================================

IF OBJECT_ID('sdm.member_denorm', 'U') IS NOT NULL DROP TABLE sdm.member_denorm;
CREATE TABLE sdm.member_denorm (
    -- Member
    MEME_CK             INT,
    SBSB_CK             INT,
    GRGR_CK             INT,
    MEME_SFX             INT,
    MEME_REL             CHAR(1),
    MEME_LAST_NAME       NVARCHAR(100),
    MEME_FIRST_NAME      NVARCHAR(100),
    MEME_MID_INIT        CHAR(1),
    MEME_SEX             CHAR(1),
    MEME_BIRTH_DT        DATE,
    MEME_SSN             BIGINT,
    MEME_MCTR_STS        NVARCHAR(10),
    MEME_ORIG_EFF_DT     DATE,
    MEME_MARITAL_STATUS  CHAR(1),
    MEME_MEDCD_NO        NVARCHAR(50),
    MEME_HICN            NVARCHAR(50),
    MEME_MCTR_RACE_NVL   NVARCHAR(50),
    MEME_MCTR_ETHN_NVL   NVARCHAR(50),

    -- Subscriber
    SBSB_ID              NVARCHAR(50),
    SBSB_LAST_NAME       NVARCHAR(100),
    SBSB_FIRST_NAME      NVARCHAR(100),
    SBSB_ORIG_EFF_DT     DATE,
    SBSB_MCTR_STS        NVARCHAR(10),
    SBSB_EMPLOY_ID       NVARCHAR(50),

    -- Subgroup
    SGSG_CK              INT,
    SBSG_EFF_DT          DATE,
    SBSG_TERM_DT         DATE,
    SBSG_MCTR_TRSN       NVARCHAR(10),
    SGSG_ID              NVARCHAR(50),
    SGSG_NAME            NVARCHAR(200),
    CSCS_ID              NVARCHAR(50),
    SGSG_STATE           CHAR(2),
    SGSG_STS             NVARCHAR(10),
    SGSG_ORIG_EFF_DT     DATE,
    SGSG_TERM_DT         DATE,

    -- Group
    GRGR_ID              NVARCHAR(50),
    GRGR_NAME            NVARCHAR(200),
    GRGR_STATE           CHAR(2),
    GRGR_COUNTY          NVARCHAR(100),
    GRGR_STS             NVARCHAR(10),
    GRGR_ORIG_EFF_DT     DATE,
    GRGR_TERM_DT         DATE,
    GRGR_MCTR_TYPE       NVARCHAR(10),

    -- Plan / Product
    PAGR_CK              INT,
    CSPI_ID              NVARCHAR(50),
    CSPD_CAT             CHAR(1),
    CSPI_EFF_DT          DATE,
    CSPI_TERM_DT         DATE,
    PDPD_ID              NVARCHAR(50),
    CSPI_SEL_IND         NVARCHAR(10),
    CSPI_HIOS_ID_NVL     NVARCHAR(50),
    CSPD_CAT_DESC        NVARCHAR(200),
    CSPD_TYPE            NVARCHAR(50),
    LOBD_ID              NVARCHAR(50),
    PDPD_RISK_IND        NVARCHAR(10),
    PDPD_MCTR_CCAT       NVARCHAR(50),
    PLDS_DESC            NVARCHAR(200),
    PDDS_DESC            NVARCHAR(200)
);

CREATE INDEX IX_member_denorm_grgr ON sdm.member_denorm (GRGR_CK);
CREATE INDEX IX_member_denorm_sgsg ON sdm.member_denorm (SGSG_CK);
CREATE INDEX IX_member_denorm_cspd ON sdm.member_denorm (CSPD_CAT);
CREATE INDEX IX_member_denorm_meme ON sdm.member_denorm (MEME_CK);


-- ============================================================================
-- SEED DATA — Lookup tables and tag vocabulary
-- ============================================================================

-- First names

INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'James', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'John', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Robert', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Michael', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'William', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'David', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Richard', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Joseph', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Thomas', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Charles', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Christopher', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Daniel', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Matthew', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Anthony', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Mark', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Donald', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Steven', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Andrew', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Paul', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Joshua', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kenneth', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kevin', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Brian', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'George', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Timothy', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ronald', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Edward', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jason', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jeffrey', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ryan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jacob', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Gary', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Nicholas', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Eric', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jonathan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Stephen', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Larry', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Justin', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Scott', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Brandon', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Benjamin', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Samuel', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Raymond', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Gregory', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Frank', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Alexander', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Patrick', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jack', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Dennis', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jerry', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Tyler', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Aaron', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Nathan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Henry', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Peter', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Adam', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Douglas', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Zachary', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Walter', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kyle', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Harold', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Carl', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Arthur', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Gerald', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Roger', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Keith', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Lawrence', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Terry', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sean', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Albert', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jesse', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Austin', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Bruce', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Willie', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ralph', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Roy', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Eugene', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Randy', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Philip', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Russell', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Bobby', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Howard', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Louis', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Wayne', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Dylan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ethan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jordan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Christian', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Mason', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Logan', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Caleb', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Luke', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Isaac', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Connor', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Elijah', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Gabriel', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Owen', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Liam', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Noah', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Aiden', N'M');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Mary', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Patricia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jennifer', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Linda', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Barbara', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Elizabeth', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Susan', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jessica', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sarah', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Karen', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Lisa', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Nancy', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Betty', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Margaret', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sandra', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ashley', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Dorothy', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kimberly', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Emily', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Donna', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Michelle', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Carol', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Amanda', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Melissa', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Deborah', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Stephanie', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Rebecca', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sharon', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Laura', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Cynthia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kathleen', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Amy', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Angela', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Shirley', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Anna', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Brenda', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Pamela', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Emma', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Nicole', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Helen', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Samantha', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Katherine', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Christine', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Debra', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Rachel', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Carolyn', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Janet', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Catherine', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Maria', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Heather', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Diane', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ruth', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Julie', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Olivia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Joyce', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Virginia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Victoria', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kelly', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Lauren', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Christina', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Joan', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Evelyn', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Judith', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Megan', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Andrea', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Cheryl', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Hannah', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jacqueline', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Martha', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Gloria', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Teresa', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ann', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sara', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Madison', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Frances', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kathryn', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Janice', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Jean', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Abigail', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Alice', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Judy', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Sophia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Grace', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Denise', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Amber', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Doris', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Marilyn', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Danielle', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Beverly', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Isabella', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Theresa', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Diana', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Natalie', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Brittany', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Charlotte', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Marie', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Kayla', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Alexis', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Lori', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ava', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Mia', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Chloe', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Ella', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Lily', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Zoe', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Leah', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Allison', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Savannah', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Audrey', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Brooklyn', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Claire', N'F');
INSERT INTO sdm.lkp_first_names (name, gender) VALUES (N'Addison', N'F');

-- Last names

INSERT INTO sdm.lkp_last_names (name) VALUES (N'Smith');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Johnson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Williams');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Brown');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Jones');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Garcia');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Miller');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Davis');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Rodriguez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Martinez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hernandez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Lopez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Gonzalez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Wilson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Anderson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Thomas');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Taylor');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Moore');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Jackson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Martin');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Lee');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Perez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Thompson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'White');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Harris');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Sanchez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Clark');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ramirez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Lewis');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Robinson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Walker');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Young');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Allen');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'King');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Wright');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Scott');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Torres');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Nguyen');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hill');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Flores');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Green');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Adams');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Nelson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Baker');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hall');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Rivera');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Campbell');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Mitchell');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Carter');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Roberts');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Gomez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Phillips');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Evans');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Turner');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Diaz');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Parker');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Cruz');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Edwards');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Collins');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Reyes');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Stewart');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Morris');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Morales');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Murphy');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Cook');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Rogers');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Gutierrez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ortiz');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Morgan');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Cooper');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Peterson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Bailey');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Reed');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Kelly');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Howard');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ramos');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Kim');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Cox');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ward');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Richardson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Watson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Brooks');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Chavez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Wood');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'James');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Bennett');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Gray');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Mendoza');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ruiz');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hughes');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Price');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Alvarez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Castillo');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Sanders');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Patel');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Myers');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Long');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ross');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Foster');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Jimenez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Powell');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Jenkins');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Perry');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Russell');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Sullivan');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Bell');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Coleman');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Butler');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Henderson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Barnes');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Gonzales');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Fisher');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Vasquez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Simmons');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Graham');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Murray');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ford');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Castro');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Marshall');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Owens');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Harrison');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Fernandez');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Patterson');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Wallace');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'West');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Webb');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Tucker');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Burns');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Crawford');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Mason');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hunt');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Warren');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Bishop');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Dixon');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Wagner');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Spencer');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Freeman');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Stone');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Horton');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hoffman');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Carpenter');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'May');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Knight');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Hart');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Ray');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Armstrong');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Day');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Montgomery');
INSERT INTO sdm.lkp_last_names (name) VALUES (N'Andrews');

-- Street names

INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Main', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Oak', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Maple', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Cedar', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Elm', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Pine', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Washington', N'Blvd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Park', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Lake', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Hill', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Church', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Spring', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'High', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Market', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Center', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Union', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Bridge', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Broad', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'River', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Walnut', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Cherry', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Meadow', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Forest', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Summit', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Valley', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Lincoln', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Franklin', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Jefferson', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Madison', N'Blvd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Adams', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Monroe', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Harrison', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Jackson', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Grant', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Garfield', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Cleveland', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Wilson', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Roosevelt', N'Blvd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Kennedy', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Reagan', N'Ct');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Sunset', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Sunrise', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Country', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Heritage', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Liberty', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Eagle', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Cardinal', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Willow', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Birch', N'Ct');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Sycamore', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Dogwood', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Magnolia', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Chestnut', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Hickory', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Ash', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Beech', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Poplar', N'St');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Spruce', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Holly', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Ivy', N'Ct');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Laurel', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Orchard', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Garden', N'Ave');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Vineyard', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Ridge', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Creek', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Brook', N'Ln');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Pond', N'Dr');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Stone', N'Rd');
INSERT INTO sdm.lkp_street_names (street, type) VALUES (N'Mill', N'Rd');

-- ZIP / City / State

INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43215', N'Columbus', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43085', N'Columbus', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43201', N'Columbus', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43220', N'Columbus', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44114', N'Cleveland', N'OH', N'Cuyahoga');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44106', N'Cleveland', N'OH', N'Cuyahoga');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44120', N'Cleveland', N'OH', N'Cuyahoga');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44130', N'Cleveland', N'OH', N'Cuyahoga');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45202', N'Cincinnati', N'OH', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45219', N'Cincinnati', N'OH', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45229', N'Cincinnati', N'OH', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45238', N'Cincinnati', N'OH', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45402', N'Dayton', N'OH', N'Montgomery');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45409', N'Dayton', N'OH', N'Montgomery');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45424', N'Dayton', N'OH', N'Montgomery');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43604', N'Toledo', N'OH', N'Lucas');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43606', N'Toledo', N'OH', N'Lucas');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43612', N'Toledo', N'OH', N'Lucas');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44308', N'Akron', N'OH', N'Summit');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44313', N'Akron', N'OH', N'Summit');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44320', N'Akron', N'OH', N'Summit');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44503', N'Youngstown', N'OH', N'Mahoning');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44512', N'Youngstown', N'OH', N'Mahoning');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45601', N'Chillicothe', N'OH', N'Ross');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45701', N'Athens', N'OH', N'Athens');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43055', N'Newark', N'OH', N'Licking');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43015', N'Delaware', N'OH', N'Delaware');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43123', N'Grove City', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43068', N'Reynoldsburg', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43081', N'Westerville', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43026', N'Hilliard', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'43235', N'Columbus', N'OH', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44035', N'Elyria', N'OH', N'Lorain');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44256', N'Medina', N'OH', N'Medina');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'44691', N'Wooster', N'OH', N'Wayne');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45066', N'Springboro', N'OH', N'Warren');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45040', N'Mason', N'OH', N'Warren');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'45069', N'West Chester', N'OH', N'Butler');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40202', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40205', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40214', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40220', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40241', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40258', N'Louisville', N'KY', N'Jefferson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40502', N'Lexington', N'KY', N'Fayette');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40508', N'Lexington', N'KY', N'Fayette');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40513', N'Lexington', N'KY', N'Fayette');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40517', N'Lexington', N'KY', N'Fayette');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41011', N'Covington', N'KY', N'Kenton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41017', N'Ft Mitchell', N'KY', N'Kenton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41042', N'Florence', N'KY', N'Boone');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41048', N'Hebron', N'KY', N'Boone');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'42001', N'Paducah', N'KY', N'McCracken');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'42101', N'Bowling Green', N'KY', N'Warren');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'42301', N'Owensboro', N'KY', N'Daviess');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40601', N'Frankfort', N'KY', N'Franklin');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40701', N'Corbin', N'KY', N'Whitley');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'40741', N'London', N'KY', N'Laurel');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41501', N'Pikeville', N'KY', N'Pike');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'41101', N'Ashland', N'KY', N'Boyd');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46204', N'Indianapolis', N'IN', N'Marion');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46220', N'Indianapolis', N'IN', N'Marion');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46237', N'Indianapolis', N'IN', N'Marion');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46250', N'Indianapolis', N'IN', N'Marion');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46268', N'Indianapolis', N'IN', N'Marion');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46802', N'Fort Wayne', N'IN', N'Allen');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46815', N'Fort Wayne', N'IN', N'Allen');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46901', N'Kokomo', N'IN', N'Howard');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'47374', N'Richmond', N'IN', N'Wayne');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'47906', N'West Lafayette', N'IN', N'Tippecanoe');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'47401', N'Bloomington', N'IN', N'Monroe');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46060', N'Noblesville', N'IN', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46032', N'Carmel', N'IN', N'Hamilton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'46143', N'Greenwood', N'IN', N'Johnson');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'47130', N'Jeffersonville', N'IN', N'Clark');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30301', N'Atlanta', N'GA', N'Fulton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30309', N'Atlanta', N'GA', N'Fulton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30318', N'Atlanta', N'GA', N'Fulton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30324', N'Atlanta', N'GA', N'DeKalb');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30340', N'Atlanta', N'GA', N'DeKalb');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30004', N'Alpharetta', N'GA', N'Fulton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30022', N'Alpharetta', N'GA', N'Fulton');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30060', N'Marietta', N'GA', N'Cobb');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30080', N'Smyrna', N'GA', N'Cobb');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30043', N'Lawrenceville', N'GA', N'Gwinnett');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30044', N'Lawrenceville', N'GA', N'Gwinnett');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30096', N'Duluth', N'GA', N'Gwinnett');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30188', N'Woodstock', N'GA', N'Cherokee');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30501', N'Gainesville', N'GA', N'Hall');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'31201', N'Macon', N'GA', N'Bibb');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'30901', N'Augusta', N'GA', N'Richmond');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'31601', N'Valdosta', N'GA', N'Lowndes');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25301', N'Charleston', N'WV', N'Kanawha');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25302', N'Charleston', N'WV', N'Kanawha');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25314', N'Charleston', N'WV', N'Kanawha');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25701', N'Huntington', N'WV', N'Cabell');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'26003', N'Wheeling', N'WV', N'Ohio');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'26501', N'Morgantown', N'WV', N'Monongalia');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'26505', N'Morgantown', N'WV', N'Monongalia');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25401', N'Martinsburg', N'WV', N'Berkeley');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'26101', N'Parkersburg', N'WV', N'Wood');
INSERT INTO sdm.lkp_zip_city_state (zip, city, state, county) VALUES (N'25801', N'Beckley', N'WV', N'Raleigh');

-- Tag vocabulary (synonym resolution for keyword search)

INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'pediatric', N'pediatric', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'pediatric', N'child', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'pediatric', N'children', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'pediatric', N'minor', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'pediatric', N'under-18', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'adult', N'adult', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'adult', N'working-age', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'senior', N'senior', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'senior', N'elderly', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'senior', N'65+', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'senior', N'medicare-age', N'age', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'new-member', N'new-member', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'new-member', N'new', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'new-member', N'recent', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'long-term', N'long-term', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'long-term', N'established', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'long-term', N'loyal', N'tenure', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'family', N'family', N'family', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'family', N'with-dependents', N'family', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'single', N'single', N'family', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'single', N'individual', N'family', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'single', N'no-dependents', N'family', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'dental', N'dental', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'dental', N'dental-plan', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medical', N'medical', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medical', N'med', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'vision', N'vision', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'vision', N'eye', N'plan', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medicaid', N'medicaid', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medicare', N'medicare', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medicare-advantage', N'medicare-advantage', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'medicare-advantage', N'ma', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'exchange', N'exchange', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'exchange', N'marketplace', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'exchange', N'aca', N'lob', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'edge-case', N'edge-case', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'edge-case', N'rare', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'edge-case', N'anomaly', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'edge-case', N'outlier', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'edge-case', N'unusual', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'high-volume', N'high-volume', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'high-volume', N'common', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'high-volume', N'dominant', N'quality', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'ohio', N'ohio', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'ohio', N'oh', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'kentucky', N'kentucky', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'kentucky', N'ky', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'indiana', N'indiana', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'indiana', N'in', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'georgia', N'georgia', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'georgia', N'ga', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'west-virginia', N'west-virginia', N'state', 0);
INSERT INTO sdm.tag_vocabulary (canonical_tag, synonym, category, sme_confirmed) VALUES (N'west-virginia', N'wv', N'state', 0);

-- Default generation rules (names and addresses — always synthetic)
INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'MEME_FIRST_NAME', N'Member First Name', N'lookup', N'lookup', N'text', 0, N'first_names', N'system');

INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'MEME_LAST_NAME', N'Member Last Name', N'lookup', N'lookup', N'text', 0, N'last_names', N'system');

INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'SBSB_FIRST_NAME', N'Subscriber First Name', N'lookup', N'lookup', N'text', 0, N'first_names', N'system');

INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'SBSB_LAST_NAME', N'Subscriber Last Name', N'lookup', N'lookup', N'text', 0, N'last_names', N'system');

INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'STREET_1', N'Street Address 1', N'lookup', N'lookup', N'text', 0, N'street_names', N'system');

INSERT INTO sdm.generation_rules (field_name, field_label, field_category, gen_method, data_type, length, lookup_source, updated_by)
VALUES (N'ZIP_CODE', N'ZIP Code', N'lookup', N'lookup', N'text', 5, N'zip_city_state', N'system');


-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'lkp_first_names' AS table_name, COUNT(*) AS row_count FROM sdm.lkp_first_names
UNION ALL SELECT 'lkp_last_names', COUNT(*) FROM sdm.lkp_last_names
UNION ALL SELECT 'lkp_street_names', COUNT(*) FROM sdm.lkp_street_names
UNION ALL SELECT 'lkp_zip_city_state', COUNT(*) FROM sdm.lkp_zip_city_state
UNION ALL SELECT 'tag_vocabulary', COUNT(*) FROM sdm.tag_vocabulary
UNION ALL SELECT 'generation_rules', COUNT(*) FROM sdm.generation_rules;

PRINT 'SDM Platform database setup complete.';
GO
