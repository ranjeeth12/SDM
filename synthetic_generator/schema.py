"""Column definitions for MEMBER_GROUP_PLAN_FLAT matching the extraction SQL."""

# 56 columns in exact SELECT order from facets_denorm_extraction.sql
COLUMNS = [
    # Member (CMC_MEME_MEMBER)
    'MEME_CK', 'SBSB_CK', 'GRGR_CK', 'MEME_SFX', 'MEME_REL',
    'MEME_LAST_NAME', 'MEME_FIRST_NAME', 'MEME_MID_INIT', 'MEME_SEX',
    'MEME_BIRTH_DT', 'MEME_SSN', 'MEME_MCTR_STS', 'MEME_ORIG_EFF_DT',
    'MEME_MARITAL_STATUS', 'MEME_MEDCD_NO', 'MEME_HICN',
    'MEME_MCTR_RACE_NVL', 'MEME_MCTR_ETHN_NVL',
    # Subscriber (CMC_SBSB_SUBSC)
    'SBSB_ID', 'SBSB_LAST_NAME', 'SBSB_FIRST_NAME', 'SBSB_ORIG_EFF_DT',
    'SBSB_MCTR_STS', 'SBSB_EMPLOY_ID',
    # Enrollment bridge (CMC_SBSG_RELATION)
    'SGSG_CK', 'SBSG_EFF_DT', 'SBSG_TERM_DT', 'SBSG_MCTR_TRSN',
    # Sub Group (CMC_SGSG_SUB_GROUP)
    'SGSG_ID', 'SGSG_NAME', 'CSCS_ID', 'SGSG_STATE', 'SGSG_STS',
    'SGSG_ORIG_EFF_DT', 'SGSG_TERM_DT',
    # Group (CMC_GRGR_GROUP)
    'GRGR_ID', 'GRGR_NAME', 'GRGR_STATE', 'GRGR_COUNTY', 'GRGR_STS',
    'GRGR_ORIG_EFF_DT', 'GRGR_TERM_DT', 'GRGR_MCTR_TYPE', 'PAGR_CK',
    # Plan (CMC_CSPI_CS_PLAN)
    'CSPI_ID', 'CSPD_CAT', 'CSPI_EFF_DT', 'CSPI_TERM_DT', 'PDPD_ID',
    'CSPI_SEL_IND', 'CSPI_HIOS_ID_NVL',
    # Plan Category (CMC_CSPD_DESC)
    'CSPD_CAT_DESC', 'CSPD_TYPE',
    # Product / LOB (CMC_PDPD_PRODUCT)
    'LOBD_ID', 'PDPD_RISK_IND', 'PDPD_MCTR_CCAT',
    # Plan / Product descriptions (from joined lookups)
    'PLDS_DESC', 'PDDS_DESC',
]

assert len(COLUMNS) == 58, f"Expected 58 columns, got {len(COLUMNS)}"

# Plan type lookup (CSPD_CAT -> attributes)
PLAN_TYPES = {
    'M': {'cspd_cat': 'M', 'cspd_cat_desc': 'Medical Product', 'cspd_type': ''},
    'D': {'cspd_cat': 'D', 'cspd_cat_desc': 'Dental Product', 'cspd_type': ''},
    'C': {'cspd_cat': 'C', 'cspd_cat_desc': 'Case Management', 'cspd_type': ''},
}

# Name pools for synthetic generation
LAST_NAMES = [
    'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller',
    'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
    'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
    'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark',
    'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King',
    'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores', 'Green',
    'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell',
    'Carter', 'Roberts',
]

MALE_FIRST = [
    'James', 'Robert', 'John', 'Michael', 'David', 'William', 'Richard',
    'Joseph', 'Thomas', 'Christopher', 'Charles', 'Daniel', 'Matthew',
    'Anthony', 'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua',
    'Kenneth', 'Kevin', 'Brian', 'George', 'Timothy', 'Ronald', 'Edward',
    'Jason', 'Jeffrey', 'Ryan',
]

FEMALE_FIRST = [
    'Mary', 'Patricia', 'Jennifer', 'Linda', 'Barbara', 'Elizabeth',
    'Susan', 'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty',
    'Margaret', 'Sandra', 'Ashley', 'Dorothy', 'Kimberly', 'Emily',
    'Donna', 'Michelle', 'Carol', 'Amanda', 'Melissa', 'Deborah',
    'Stephanie', 'Rebecca', 'Sharon', 'Laura', 'Cynthia',
]

CHILD_MALE = [
    'Liam', 'Noah', 'Oliver', 'Elijah', 'Lucas', 'Mason', 'Logan',
    'Alexander', 'Ethan', 'Jacob', 'Benjamin', 'Henry', 'Sebastian',
    'Jack', 'Aiden',
]

CHILD_FEMALE = [
    'Olivia', 'Emma', 'Charlotte', 'Amelia', 'Sophia', 'Isabella',
    'Mia', 'Evelyn', 'Harper', 'Luna', 'Ella', 'Avery', 'Scarlett',
    'Sofia', 'Riley',
]

MID_INITIALS = list('ABCDEFGHJKLMNPRSTW')
RACE_CODES = ['WHIT', 'BLCK', 'ASIN', 'HISP', 'MULT', 'OTHR']
ETHN_CODES = ['NOHI', 'HISP']
