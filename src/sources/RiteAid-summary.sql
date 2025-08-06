with date_range as (
select
'{period_start}'::date AS period_start,
'{period_end}'::date AS period_end
),
fills as (
  SELECT
    c.prescription_reference_number AS "Rx#",
    ph.ncpdp as "NCPDP",
    c.bin_number AS "Unique Identifier",
    date(c.valid_from)::text as "Fill Date",
    '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '20' then 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '07' then 'MAC'
      WHEN c.basis_of_reimbursement_determination_resp = '03' then 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '04' then 'UNC'
      ELSE 'unexpected_basis_of_reimbursement'
    END AS fill_type,
    CASE
      WHEN c.claim_date_of_service::date < '2025-04-01'::date THEN
        CASE
          WHEN ndcsh.nadac_is_generic = 'True' THEN 'G'
          WHEN fill_type = 'AWP' AND ndcsh.multi_source_code = 'Y' THEN 'G'
          WHEN fill_type = 'AWP'
               AND ndcsh.multi_source_code = 'M' AND ndcsh.name_type_code = 'G' THEN 'G'
          ELSE 'B'
        END
      ELSE
        CASE
          WHEN ndcsh.multi_source_code = 'O'
               AND c.dispense_as_written IN (
                 'SUBSTITUTION ALLOWED PHARMACIST SELECTED PRODUCT DISPENSED',
                 'SUBSTITUTION ALLOWED GENERIC DRUG NOT IN STOCK',
                 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC',
                 'OVERRIDE'
               ) THEN 'G'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'G'
          ELSE 'B'
        END
    END AS "Drug Type Indicator",
    c.quantity_dispensed AS "Total Quantity Dispensed",
    (c.ingredient_cost_paid_resp::BIGINT / 100::decimal(3, 0))::decimal(9, 2) AS "Ingredient cost paid",
    (c.total_paid_response::BIGINT / 100::decimal(3, 0))::decimal(6, 2) AS "Total paid response",
    (c.dispensing_fee_paid_resp::BIGINT / 100::decimal(3, 0))::decimal(6, 2) AS "Dispensing fee paid",
    CASE
      WHEN c.claim_date_of_service::date < '2025-04-01'::date THEN
        CASE
          WHEN fill_type = 'NADAC' THEN '8.00'::decimal(3, 2)
          ELSE '1.75'::decimal(3, 2)
        END
      ELSE
        CASE
          WHEN c.days_supply <= 60 THEN
            CASE
              WHEN fill_type = 'NADAC' THEN '8.50'::decimal(3, 2)
              ELSE '1.01'::decimal(3, 2)
            END
          ELSE
            CASE
              WHEN fill_type = 'NADAC' THEN '14.50'::decimal(4, 2)
              ELSE '1.01'::decimal(4, 2)
            END
        END
    END AS "Contracted_disp_fee",
    CASE
      WHEN fill_type = 'NADAC' THEN (NULLIF(nch.nadac, '')::decimal(12, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN "Drug Type Indicator" = 'G' THEN (NULLIF(nch.awp, '')::decimal(12, 4) * 0.7::decimal(2,1) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE (NULLIF(nch.awp, '')::decimal(12, 4) * 0.83::decimal(3,2) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
    END AS "Contracted_AWP",
    (NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "Average wholesale price (AWP)",
    (NULLIF(nch.awp, '')::decimal(18, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "AWP unit cost",
    (NULLIF(nch.nadac, '')::decimal(18, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "NADAC unit cost",
    CASE
      WHEN fill_type = 'NADAC'
        THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE 0
    END AS contracted_nadactr,
    (c.usual_and_customary_charge::BIGINT / 100::decimal(3, 0))::decimal(12, 2) AS "Dispensing pharmacy U&C Price",
    '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
    c.network_reimbursement_id AS "Network Id",
    c.process_control_number as "PCN",
   CASE
     WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
     WHEN ndcsh.is_otc = 'True' THEN 'OTC'
     ELSE ' '
   END AS "Exclusion Reason",
    CASE
      WHEN "Exclusion Reason" IN ('UNC', 'OTC') THEN 'Y'
      ELSE 'N'
    END AS "Excluded Claim Indicator",
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '20' THEN 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
    END AS basis_of_reimbursement_source,
    c.transaction_code AS "Reversal indicator",
    CASE
      WHEN fill_type = 'NADAC' THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN "Drug Type Indicator" = 'G' THEN (NULLIF(nch.awp, '')::decimal(18, 4) * 0.7::decimal(2,1) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE (NULLIF(nch.awp, '')::decimal(18, 4) * 0.83::decimal(2,2) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
    END AS "Contracted Ingredient Cost",
    CASE
      WHEN fill_type = 'NADAC'
        THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN fill_type = 'AWP'
        THEN (NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE 0
    END AS "Ingredient Cost",
    COALESCE(c.amount_of_copay::BIGINT, 0) AS "Copay Amount",
    (c.ingredient_cost_paid_resp + c.dispensing_fee_paid_resp + c.amount_attributed_to_sales_tax_response + COALESCE(c.amount_of_copay, 0))::BIGINT * 0.01::decimal(4, 2) AS "Total Reimbursement to Pharmacy",
    (-c.total_paid_response::BIGINT / 100::decimal(3,0))::decimal(6, 2) AS "Administration Fee",
    fill_type AS "Pricing Type"
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
  LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
  LEFT JOIN medications_v2.drug d on d.drug_descriptor_id = mfmf.drug_descriptor_id
  WHERE ph.chain_name = 'rite_aid'
  AND c.authorization_number NOT ILIKE 'U%'
  AND c.fill_status = 'filled'
  AND c.valid_from::date >= (select period_start from date_range)
  AND c.valid_to::date > (select period_end from date_range)
  AND c.valid_from::date <= (select period_end from date_range)
  ORDER BY c.valid_from DESC
  ),
  reversals as (
   SELECT
    c.prescription_reference_number AS "Rx#",
    ph.ncpdp as "NCPDP",
    c.bin_number AS "Unique Identifier",
    date(c.valid_from)::text as "Fill Date",
    '="' || c.product_id || '"' AS "NDC", --needed for excel to treat data as text not number
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '20' then 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '07' then 'MAC'
      WHEN c.basis_of_reimbursement_determination_resp = '03' then 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '04' then 'UNC'
      ELSE 'unexpected_basis_of_reimbursement'
    END AS fill_type,
    CASE
      WHEN c.claim_date_of_service::date < '2025-04-01'::date THEN
        CASE
          WHEN ndcsh.nadac_is_generic = 'True' THEN 'G'
          WHEN fill_type = 'AWP' AND ndcsh.multi_source_code = 'Y' THEN 'G'
          WHEN fill_type = 'AWP'
               AND ndcsh.multi_source_code = 'M' AND ndcsh.name_type_code = 'G' THEN 'G'
          ELSE 'B'
        END
      ELSE
        CASE
          WHEN ndcsh.multi_source_code = 'O'
               AND c.dispense_as_written IN (
                 'SUBSTITUTION ALLOWED PHARMACIST SELECTED PRODUCT DISPENSED',
                 'SUBSTITUTION ALLOWED GENERIC DRUG NOT IN STOCK',
                 'SUBSTITUTION ALLOWED BRAND DRUG DISPENSED AS A GENERIC',
                 'OVERRIDE'
               ) THEN 'G'
          WHEN ndcsh.multi_source_code = 'Y' THEN 'G'
          ELSE 'B'
        END
    END AS "Drug Type Indicator",
    c.quantity_dispensed AS "Total Quantity Dispensed",
    (c.ingredient_cost_paid_resp::BIGINT / 100::decimal(3, 0))::decimal(9, 2) AS "Ingredient cost paid",
    (c.total_paid_response::BIGINT / 100::decimal(3, 0))::decimal(6, 2) AS "Total paid response",
    (c.dispensing_fee_paid_resp::BIGINT / 100::decimal(3, 0))::decimal(6, 2) AS "Dispensing fee paid",
    CASE
      WHEN c.claim_date_of_service::date < '2025-04-01'::date THEN
        CASE
          WHEN fill_type = 'NADAC' THEN '8.00'::decimal(3, 2)
          ELSE '1.75'::decimal(3, 2)
        END
      ELSE
        CASE
          WHEN c.days_supply <= 60 THEN
            CASE
              WHEN fill_type = 'NADAC' THEN '8.50'::decimal(3, 2)
              ELSE '1.01'::decimal(3, 2)
            END
          ELSE
            CASE
              WHEN fill_type = 'NADAC' THEN '14.50'::decimal(4, 2)
              ELSE '1.01'::decimal(4, 2)
            END
        END
    END AS "Contracted_disp_fee",
    CASE
      WHEN fill_type = 'NADAC' THEN (NULLIF(nch.nadac, '')::decimal(12, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN "Drug Type Indicator" = 'G' THEN (NULLIF(nch.awp, '')::decimal(12, 4) * 0.7::decimal(2,1) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE (NULLIF(nch.awp, '')::decimal(12, 4) * 0.83::decimal(3,2) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
    END AS "Contracted_AWP",
    (NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "Average wholesale price (AWP)",
    (NULLIF(nch.awp, '')::decimal(18, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "AWP unit cost",
    (NULLIF(nch.nadac, '')::decimal(18, 4) / 100::decimal(3, 0))::decimal(18, 4) AS "NADAC unit cost",
    CASE
      WHEN fill_type = 'NADAC'
        THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE 0
    END AS contracted_nadactr,
    (c.usual_and_customary_charge::BIGINT / 100::decimal(3, 0))::decimal(12, 2) AS "Dispensing pharmacy U&C Price",
    '="' || c.authorization_number || '"' AS "Claim Authorization Number", --needed for excel to treat data as text not number
    c.network_reimbursement_id AS "Network Id",
    c.process_control_number as "PCN",
   CASE
     WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
     WHEN ndcsh.is_otc = 'True' THEN 'OTC'
     ELSE ' '
   END AS "Exclusion Reason",
    CASE
      WHEN "Exclusion Reason" IN ('UNC', 'OTC') THEN 'Y'
      ELSE 'N'
    END AS "Excluded Claim Indicator",
    CASE
      WHEN c.basis_of_reimbursement_determination_resp = '03' THEN 'AWP'
      WHEN c.basis_of_reimbursement_determination_resp = '07' THEN 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '20' THEN 'NADAC'
      WHEN c.basis_of_reimbursement_determination_resp = '04' THEN 'UNC'
    END AS basis_of_reimbursement_source,
    'B2' AS "Reversal indicator",
    CASE
      WHEN fill_type = 'NADAC' THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN "Drug Type Indicator" = 'G' THEN (NULLIF(nch.awp, '')::decimal(18, 4) * 0.7::decimal(2,1) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE (NULLIF(nch.awp, '')::decimal(18, 4) * 0.83::decimal(2,2) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
    END AS "Contracted Ingredient Cost",
    CASE
      WHEN fill_type = 'NADAC'
        THEN (NULLIF(nch.nadac, '')::decimal(18, 4) * c.quantity_dispensed::decimal(10, 4) / 100::decimal(3, 0))::decimal(18, 4)
      WHEN fill_type = 'AWP'
        THEN (NULLIF(nch.awp, '')::decimal(18, 4) * c.quantity_dispensed::decimal(12, 4) / 100::decimal(3, 0))::decimal(18, 4)
      ELSE 0
    END AS "Ingredient Cost",
    COALESCE(c.amount_of_copay::BIGINT, 0) AS "Copay Amount",
    (c.ingredient_cost_paid_resp + c.dispensing_fee_paid_resp + c.amount_attributed_to_sales_tax_response + COALESCE(c.amount_of_copay, 0))::BIGINT * 0.01::decimal(4, 2) AS "Total Reimbursement to Pharmacy",
    (-c.total_paid_response::BIGINT / 100::decimal(3,0))::decimal(6, 2) AS "Administration Fee",
    fill_type AS "Pricing Type"
  FROM reporting.claims c
  LEFT JOIN historical_data.pharmacy_history ph ON ph.id = c.npi AND c.valid_from::timestamp >= ph.valid_from::timestamp AND c.valid_from::timestamp < ph.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndcs_v2_history ndcsh ON ndcsh.id = c.product_id AND c.claim_date_of_service::timestamp + interval '12 hours' >= ndcsh.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < ndcsh.valid_to::timestamp
  LEFT JOIN historical_data_bin_019876.ndc_costs_v2_history nch ON nch.ndc = ndcsh.id AND c.claim_date_of_service::timestamp + interval '12 hours' >= nch.valid_from::timestamp AND c.claim_date_of_service::timestamp + interval '12 hours' < nch.valid_to::timestamp
  LEFT JOIN mf.mf2ndc mfmf ON mfmf.ndc_upc_hri::BIGINT = c.product_id::BIGINT
  LEFT JOIN mf.mf2name mfnm ON mfnm.drug_descriptor_id = mfmf.drug_descriptor_id
  LEFT JOIN medications_v2.drug d on d.drug_descriptor_id = mfmf.drug_descriptor_id
  WHERE ph.chain_name = 'rite_aid'
  AND c.authorization_number NOT ILIKE 'U%'
  AND c.fill_status = 'filled'
  AND c.valid_from < (select period_start from date_range)
  AND c.valid_to::date >= (select period_start from date_range)
  AND c.valid_to::date <= (select period_end from date_range)
  ORDER BY c.valid_from DESC
  )
  SELECT * from fills
  UNION ALL
  SELECT * from reversals
