-- ================================================================
--  FACETS DENORMALIZATION — EXTRACTION SCRIPTS
--  CareSource TDM / SDM POC
--  Database: FACPRDDB (TDM safe-room de-identified copy)
-- ================================================================


-- ================================================================
--  MODEL 1: MEMBER_GROUP_PLAN_FLAT
--
--  Grain   : one row per member × sub-group enrollment period
--             × available plan in that coverage section
--  Anchor  : CMC_MEME_MEMBER
--  Join seq: MEME_MEMBER
--              → SBSB_SUBSC      (SBSB_CK + GRGR_CK)
--              → SBSG_RELATION   (SBSB_CK + GRGR_CK)   ← grain table
--              → SGSG_SUB_GROUP  (SGSG_CK + GRGR_CK)
--              → GRGR_GROUP      (GRGR_CK)
--              → CSPI_CS_PLAN    (GRGR_CK + CSCS_ID)    ← row-expanding
--              → CSPD_DESC       (CSPD_CAT)
--              → PDPD_PRODUCT    (PDPD_ID)
--
--  NOTE: CMC_MEPE_PLAN_ENR is not available in this DDL set.
--  Without it, there is no direct member → specific plan link.
--  Step 5 (CSPI_CS_PLAN) expands via the CSCS_ID bridge shared
--  between SGSG_SUB_GROUP and CSPI_CS_PLAN, producing one row
--  per plan offered to the sub-group's coverage section.
--  To approximate actual enrollment, add a date-overlap filter:
--    p.CSPI_EFF_DT <= r.SBSG_EFF_DT
--    AND (p.CSPI_TERM_DT >= r.SBSG_TERM_DT OR p.CSPI_TERM_DT IS NULL)
-- ================================================================

SELECT
    -- ── Member (CMC_MEME_MEMBER) ─────────────────────────────
    m.MEME_CK,
    m.SBSB_CK,
    m.GRGR_CK,
    m.MEME_SFX,
    m.MEME_REL,
    m.MEME_LAST_NAME,
    m.MEME_FIRST_NAME,
    m.MEME_MID_INIT,
    m.MEME_SEX,
    m.MEME_BIRTH_DT,
    m.MEME_SSN,
    m.MEME_MCTR_STS,
    m.MEME_ORIG_EFF_DT,
    m.MEME_MARITAL_STATUS,
    m.MEME_MEDCD_NO,
    m.MEME_HICN,
    m.MEME_MCTR_RACE_NVL,
    m.MEME_MCTR_ETHN_NVL,

    -- ── Subscriber (CMC_SBSB_SUBSC) ──────────────────────────
    sb.SBSB_ID,
    sb.SBSB_LAST_NAME,
    sb.SBSB_FIRST_NAME,
    sb.SBSB_ORIG_EFF_DT,
    sb.SBSB_MCTR_STS         AS SBSB_MCTR_STS,
    sb.SBSB_EMPLOY_ID,

    -- ── Enrollment bridge (CMC_SBSG_RELATION) ─────────────────
    --   These two columns define the enrollment period grain
    r.SGSG_CK,
    r.SBSG_EFF_DT,
    r.SBSG_TERM_DT,
    r.SBSG_MCTR_TRSN,

    -- ── Sub Group (CMC_SGSG_SUB_GROUP) ────────────────────────
    sg.SGSG_ID,
    sg.SGSG_NAME,
    sg.CSCS_ID,              -- Coverage Section — bridge key to plan
    sg.SGSG_STATE,
    sg.SGSG_STS,
    sg.SGSG_ORIG_EFF_DT,
    sg.SGSG_TERM_DT,

    -- ── Group (CMC_GRGR_GROUP) ────────────────────────────────
    g.GRGR_ID,
    g.GRGR_NAME,
    g.GRGR_STATE,
    g.GRGR_COUNTY,
    g.GRGR_STS,
    g.GRGR_ORIG_EFF_DT,
    g.GRGR_TERM_DT,
    g.GRGR_MCTR_TYPE,
    g.PAGR_CK,               -- parent group CK — enables group hierarchy

    -- ── Plan (CMC_CSPI_CS_PLAN) ───────────────────────────────
    --   Joined via GRGR_CK + CSCS_ID (shared with SGSG_SUB_GROUP)
    p.CSPI_ID,
    p.CSPD_CAT,
    p.CSPI_EFF_DT,
    p.CSPI_TERM_DT,
    p.PDPD_ID,
    p.CSPI_SEL_IND,
    p.CSPI_HIOS_ID_NVL,

    -- ── Plan Category (CMC_CSPD_DESC) ─────────────────────────
    pd.CSPD_CAT_DESC,
    pd.CSPD_TYPE,

    -- ── Product / LOB (CMC_PDPD_PRODUCT) ─────────────────────
    pr.LOBD_ID,
    pr.PDPD_RISK_IND,
    pr.PDPD_MCTR_CCAT

FROM       CMC_MEME_MEMBER    m

-- Step 1: Subscriber — member belongs to subscriber within group
INNER JOIN CMC_SBSB_SUBSC    sb  ON  sb.SBSB_CK   = m.SBSB_CK
                                 AND sb.GRGR_CK    = m.GRGR_CK

-- Step 2: Enrollment bridge — subscriber enrolled in a sub-group
--         SBSG_EFF_DT / SBSG_TERM_DT define the enrollment period
--         This join is the grain-defining step
INNER JOIN CMC_SBSG_RELATION  r  ON  r.SBSB_CK    = sb.SBSB_CK
                                 AND r.GRGR_CK     = sb.GRGR_CK

-- Step 3: Sub Group — carries CSCS_ID which bridges to plan
INNER JOIN CMC_SGSG_SUB_GROUP sg ON  sg.SGSG_CK   = r.SGSG_CK
                                 AND sg.GRGR_CK    = r.GRGR_CK

-- Step 4: Group — top-level employer/account record
INNER JOIN CMC_GRGR_GROUP     g  ON  g.GRGR_CK    = sg.GRGR_CK

-- Step 5: Plan — CSCS_ID is the three-way bridge:
--         SGSG_SUB_GROUP.CSCS_ID = GRGR_GROUP.CSCS_ID = CSPI_CS_PLAN.CSCS_ID
--         This join EXPANDS rows: one row per plan available in the
--         coverage section.  Without MEPE, it cannot be narrowed
--         to the single plan the member is actually enrolled in.
INNER JOIN CMC_CSPI_CS_PLAN   p  ON  p.GRGR_CK    = sg.GRGR_CK
                                 AND p.CSCS_ID     = sg.CSCS_ID

-- Step 6: Plan Category description
INNER JOIN CMC_CSPD_DESC      pd ON  pd.CSPD_CAT   = p.CSPD_CAT

-- Step 7: Product — Line of Business, risk indicators
INNER JOIN CMC_PDPD_PRODUCT   pr ON  pr.PDPD_ID    = p.PDPD_ID

-- ── Recommended filters ──────────────────────────────────────
-- WHERE g.GRGR_STS   = 'AC'               -- active groups only
-- AND   sg.SGSG_STS  = 'AC'               -- active sub-groups
-- AND   r.SBSG_TERM_DT >= GETDATE()       -- active enrollments
-- AND   p.CSPI_TERM_DT >= GETDATE()       -- active plans
;


-- ================================================================
--  MODEL 2: CLAIM_LINE_FLAT
--
--  Grain   : one row per claim service line (CLCL_ID + BLCL_SEQ_NO)
--  Anchor  : CMC_BLCL_CLM_DTL
--  Join seq: BLCL_CLM_DTL
--              → CLCL_CLAIM      (CLCL_ID)
--              → MEME_MEMBER     (MEME_CK)
--              → PRPR_PROV       (PRPR_ID)
--              → GRGR_GROUP      (GRGR_CK)
--              → SGSG_SUB_GROUP  (SGSG_CK + GRGR_CK)
--              → CSPI_CS_PLAN    (GRGR_CK + CSCS_ID + CSPI_ID + CSPD_CAT)
--              → CSPD_DESC       (CSPD_CAT)
--              → PDPD_PRODUCT    (PDPD_ID)
--         LEFT → BPIL_CLM_DTL   (CLCL_ID + CDML_SEQ_NO)  ← ITS only
--
--  NOTE: CMC_CLCL_CLAIM carries a full denormalized snapshot of
--  group/plan context at adjudication time (GRGR_CK, SGSG_CK,
--  CSCS_ID, CSPI_ID, CSPD_CAT, PDPD_ID).  These are used
--  directly for lookups — no need to re-traverse SBSG_RELATION.
--
--  CSPI_CS_PLAN requires all four key columns from the claim header:
--    GRGR_CK + CSCS_ID + CSPI_ID + CSPD_CAT
--
--  BPIL is LEFT-joined: only ITS inter-plan transfer claims have
--  rows in BPIL.  All BPIL columns will be NULL for non-ITS claims.
-- ================================================================

SELECT
    -- ── Claim Line PK ────────────────────────────────────────
    bl.BLEI_CK,
    bl.BLCL_SEQ_NO,
    bl.CLCL_ID,
    bl.CDML_SEQ_NO,              -- joins to BPIL for ITS line pricing

    -- ── Claim Header (CMC_CLCL_CLAIM) ────────────────────────
    cl.MEME_CK,
    cl.GRGR_CK,
    cl.SBSB_CK,
    cl.SGSG_CK,
    cl.PRPR_ID,
    cl.CSPI_ID,
    cl.PDPD_ID,
    cl.CSPD_CAT,
    cl.CSCS_ID,
    cl.CLCL_CL_TYPE,
    cl.CLCL_CL_SUB_TYPE,
    cl.CLCL_CUR_STS,
    cl.CLST_MCTR_REAS,
    cl.CLCL_INPUT_DT,
    cl.CLCL_RECD_DT,
    cl.CLCL_PAID_DT,
    cl.CLCL_LOW_SVC_DT,
    cl.CLCL_HIGH_SVC_DT,
    cl.CLCL_TOT_CHG,
    cl.CLCL_TOT_PAYABLE,
    cl.CLCL_NTWK_IND,
    cl.CLCL_PCP_IND,
    cl.CLCL_CAP_IND,
    cl.CLCL_ME_AGE,              -- member age at adjudication (pre-computed)
    cl.MEME_REL,
    cl.MEME_SEX,
    cl.CLCL_INPUT_METH,

    -- ── Member snapshot (CMC_MEME_MEMBER) ────────────────────
    --   Point-in-time snapshot — not the value at adjudication date
    m.MEME_LAST_NAME,
    m.MEME_FIRST_NAME,
    m.MEME_BIRTH_DT,
    m.MEME_SSN,
    m.MEME_MCTR_STS,
    m.MEME_ORIG_EFF_DT,
    m.MEME_MEDCD_NO,

    -- ── Group (CMC_GRGR_GROUP) ────────────────────────────────
    g.GRGR_ID,
    g.GRGR_NAME,
    g.GRGR_STATE,

    -- ── Sub Group (CMC_SGSG_SUB_GROUP) ────────────────────────
    sg.SGSG_ID,
    sg.SGSG_NAME,

    -- ── Plan Category + Product ───────────────────────────────
    pd.CSPD_CAT_DESC,
    pd.CSPD_TYPE,
    pr.LOBD_ID,
    pr.PDPD_RISK_IND,

    -- ── Provider (CMC_PRPR_PROV) ─────────────────────────────
    pv.PRPR_NAME,
    pv.PRPR_NPI,
    pv.PRPR_ENTITY,
    pv.PRCF_MCTR_SPEC,
    pv.PRCF_MCTR_SPEC2,
    pv.PRPR_STS,
    pv.PRPR_TAXONOMY_CD,

    -- ── Claim Line — Billing (CMC_BLCL_CLM_DTL) ──────────────
    bl.BLCL_FROM_DT,
    bl.BLCL_TO_DT,
    bl.IDCD_ID,                  -- ICD-10 principal diagnosis
    bl.IDCD_ID_REL,              -- ICD-10 related condition
    bl.IPCD_ID,                  -- CPT / procedure code
    bl.DPDP_ID,                  -- DRG
    bl.LOBD_ID                   AS BLCL_LOBD_ID,
    bl.BLCL_ACCT_CAT,
    bl.BLCL_EXP_CAT,
    bl.BLCL_CHG_AMT,
    bl.BLCL_CONSIDER_CHG,
    bl.BLCL_ALLOW,
    bl.BLCL_PAID_AMT,
    bl.BLCL_DISC_AMT,
    bl.BLCL_CAP_IND,
    bl.BLCL_EXTRACONT_IND,
    bl.BLCL_SURCH_AMT,
    bl.AFCP_FAM_SL_AMT,
    bl.BLCL_INDIV_SL_AMT,

    -- ── ITS Pricing (CMC_BPIL_CLM_DTL) — LEFT JOIN ───────────
    --   NULL for all non-ITS claims
    bp.CKPY_REF_ID,
    bp.CDMI_PRCE_METH,
    bp.CDMI_PRCE_PRI_RULE,
    bp.CDMI_SF_PRICE,
    bp.CDPP_CLASS_PROV,
    bp.CDMI_CALC_DIS_AMT,
    bp.CDMI_SURCHG_PCT,
    bp.CDMI_SURCHG_AMT,
    bp.ITCD_ITS_CODE1,
    bp.ITCD_DESC1,
    bp.ITCD_ITS_CODE2,
    bp.ITCD_DESC2,
    bp.ITCD_ITS_CODE3,
    bp.ITCD_DESC3,
    bp.BPIL_CDOR_XD_AMT,
    bp.BPIL_CDOR_XD_EXCD

FROM       CMC_BLCL_CLM_DTL  bl

-- Step 1: Claim header — provides all FK context
INNER JOIN CMC_CLCL_CLAIM     cl  ON  cl.CLCL_ID   = bl.CLCL_ID

-- Step 2: Member demographics
INNER JOIN CMC_MEME_MEMBER    m   ON  m.MEME_CK    = cl.MEME_CK

-- Step 3: Billing provider
INNER JOIN CMC_PRPR_PROV      pv  ON  pv.PRPR_ID   = cl.PRPR_ID

-- Step 4: Group
INNER JOIN CMC_GRGR_GROUP     g   ON  g.GRGR_CK    = cl.GRGR_CK

-- Step 5: Sub Group — BOTH keys required
INNER JOIN CMC_SGSG_SUB_GROUP sg  ON  sg.SGSG_CK   = cl.SGSG_CK
                                  AND sg.GRGR_CK    = cl.GRGR_CK

-- Step 6: Plan — ALL FOUR composite key columns required
--         CLCL_CLAIM carries all four at adjudication snapshot
INNER JOIN CMC_CSPI_CS_PLAN   p   ON  p.GRGR_CK    = cl.GRGR_CK
                                  AND p.CSCS_ID     = cl.CSCS_ID
                                  AND p.CSPI_ID     = cl.CSPI_ID
                                  AND p.CSPD_CAT    = cl.CSPD_CAT

-- Step 7: Plan Category description
INNER JOIN CMC_CSPD_DESC      pd  ON  pd.CSPD_CAT   = cl.CSPD_CAT

-- Step 8: Product / LOB
INNER JOIN CMC_PDPD_PRODUCT   pr  ON  pr.PDPD_ID    = cl.PDPD_ID

-- Step 9: ITS line pricing — LEFT JOIN, NULL for non-ITS claims
--         Match on CLCL_ID + CDML_SEQ_NO (line-level sequence)
LEFT  JOIN CMC_BPIL_CLM_DTL   bp  ON  bp.CLCL_ID    = bl.CLCL_ID
                                  AND bp.CDML_SEQ_NO  = bl.CDML_SEQ_NO

-- ── Recommended filters ──────────────────────────────────────
-- WHERE cl.CLCL_CUR_STS NOT IN ('V', 'X')      -- exclude void/deleted
-- AND   cl.CLCL_LOW_SVC_DT >= '2022-01-01'     -- date range
-- AND   cl.CLCL_LOW_SVC_DT <  '2025-01-01'
-- AND   cl.CLCL_CL_TYPE IN ('M', 'H')          -- medical and hospital
;
