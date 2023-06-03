/* ----------------------------------------------------------------- */
/*                      Create dwh schema table                      */
/* ----------------------------------------------------------------- */
CREATE TABLE dwh.fct_loss_prevention_sale (
    date_key int4 NOT NULL
    , store_key int4 NOT NULL
    , item_key int4 NOT NULL
    , time_key int4 NOT NULL
    , customer_key int8 NULL
    , primary_store_key int4 NULL
    , cashier_primary_store_key int4 NULL
    , source_system text NOT NULL
    , sale_line_sid text NOT NULL
    , invoice_sid text NOT NULL
    , receipt_number text NULL
    , customer_sid text NULL
    , cashier_id text NULL
    , employee_id text NULL
    , original_invoice_sid text NOT NULL
    , original_sale_line_sid text NULL
    , billing_address_id text NULL
    , shipping_address_id text NULL
    , sale_channel text NULL
    , is_employee_flag boolean NULL
    , in_employment_period_flag boolean NULL
    , return_in_return_period boolean NULL
    , returned_greater_than_paid_flag boolean NULL
    , employee_ea_flag boolean NULL
    , record_created timestamptz NOT NULL DEFAULT now()
    , record_updated timestamptz NOT NULL DEFAULT now()
    , CONSTRAINT fct_loss_prevention_sale_pk PRIMARY KEY (sale_line_sid, source_system)
);

CREATE INDEX fct_loss_prevention_sale_date_key_ix ON dwh.fct_loss_prevention_sale USING btree (date_key);
CREATE INDEX fct_loss_prevention_sale_store_key_ix ON dwh.fct_loss_prevention_sale USING btree (store_key);
CREATE INDEX fct_loss_prevention_sale_item_key_ix ON dwh.fct_loss_prevention_sale USING btree (item_key);
CREATE INDEX fct_loss_prevention_sale_employee_id_ix ON dwh.fct_loss_prevention_sale USING btree (employee_id);
CREATE INDEX fct_loss_prevention_sale_record_updated_ix ON dwh.fct_loss_prevention_sale USING btree (record_updated);

-- Comments --;
COMMENT ON TABLE dwh.fct_loss_prevention_sale IS 'Description: xxx description.
Subject Area: xxx business/function.
Updated: xxx refresh frequency.
2023-06-02 - xxx - Created the table.';

COMMENT ON COLUMN dwh.fct_loss_prevention_sale.date_key IS 'Unique ID for date.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.store_key IS 'Unique ID for store.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.item_key IS 'Unique ID for item.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.time_key IS 'Unique ID for time.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.customer_key IS 'Unique ID for customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.primary_store_key IS 'Store key of customer employee''s primary store.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.cashier_primary_store_key IS 'Store key of cashier''s primary store.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.source_system IS 'System from which the customer record originated from.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.sale_line_sid IS 'Unique identifier for the sale line in the source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.invoice_sid IS 'Unique identifier for the transaction in the source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.receipt_number IS 'Receipt number as recorded on the receipt.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.customer_sid IS 'Unique customer identifier from source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.cashier_id IS 'Employee ID that processed the transaction.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.employee_id IS 'Employee ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.original_invoice_sid IS 'Unique identifier for the transaction in the source system of the original purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.original_sale_line_sid IS 'Unique identifier for the sale line in the source system of the original purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.billing_address_id IS 'Billing address ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.shipping_address_id IS 'Shipping address ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.sale_channel IS '"Store" or "Online".';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.is_employee_flag IS 'True if purchase uses employee discount.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.in_employment_period_flag IS 'True if transaction date between employee hire and termination dates.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.return_in_return_period IS 'True if:reason code in (''CRF'',''CRR'',    ''CRO'', ''DIS'', ''DIT'') for damaged/reaction/incorrectreturned within 90    days for customersreturned within 30    days for employee purchases to any tender or 90 days to a gift card or    used in another purchaseNULL for non-returns and returns without an invoice of purchaseReturn Policy for customers from our website:Return Policy for employee transactions:.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.returned_greater_than_paid_flag IS 'True if the customer received a greater return amount than originally paid.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.employee_ea_flag IS 'True if the customer is an employee and used Endless Asle.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.record_created IS 'Date record created in table.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale.record_updated IS 'Date record updated in table.';

-- Table parameters --;
INSERT INTO adm.table_parameters(table_name, increment_from, overlap) values('dwh.fct_loss_prevention_sale', '1997-12-21', '0 minutes');

-- Permissions --;
ALTER TABLE dwh.fct_loss_prevention_sale OWNER to role_master;
GRANT SELECT ON dwh.fct_loss_prevention_sale TO role_select_dwh;

/* ----------------------------------------------------------------- */
/*                     Create view in dwh schema                     */
/* ----------------------------------------------------------------- */
CREATE MATERIALIZED VIEW
    dwh.fct_loss_prevention_sale_v
AS SELECT
    flps.date_key
    , dd.date
    , flps.time_key
    , dt.time
    , flps.source_system
    , flps.sale_line_sid
    , flps.invoice_sid
    , flps.receipt_number
    , fs.extended_price_lcy
    , fs.quantity
    , fs.receipt_timestamp
    , flps.original_invoice_sid
    , flps.original_sale_line_sid
    , fs2.extended_price_lcy AS original_extended_price_lcy
    , flps.returned_greater_than_paid_flag
    , fs2.receipt_timestamp AS original_receipt_timestamp
    , flps.return_in_return_period
    , fs.return_reason_code
    , flps.customer_key
    , flps.customer_sid
    , dcpd.county AS customer_state
    , flps.billing_address_id
    , fsop.billing_postcode AS billing_address_postcode
    , flps.shipping_address_id
    , fsop.shipping_postcode AS shipping_address_postcode
    , flps.is_employee_flag
    , flps.employee_id
    , flps.in_employment_period_flag
    , de.business_title
    , de.employee_type
    , de.job_family_group
    , de.cost_centre
    , flps.employee_ea_flag
    , fs.order_type
    , flps.primary_store_key
    , de.primary_store AS primary_store_code
    , ds1.store_name AS primary_store_name
    , flps.store_key
    , flps.sale_channel
    , ds.store_code
    , ds.store_name
    , ds.county AS store_state
    , ds.country AS store_country
    , flps.item_key
    , di.item_code
    , di.description AS item_description
    , di.category_name
    , di.subcategory_name
    , di.pim_brand_name
    , di.pim_product_name
    , di.pim_shade_name
    , flps.cashier_id
    , de1.business_title AS cashier_business_title
    , de1.cost_centre AS cashier_cost_centre
    , ds2.store_code AS cashier_primary_store_code
    , ds2.county AS cashier_primary_store_state
    , fs.order_fulfilment_location_code
FROM
    dwh.fct_loss_prevention_sale flps
LEFT JOIN
    dwh.dim_date dd ON dd.xxx = flps.xxx
LEFT JOIN
    dwh.dim_time dt ON dt.xxx = flps.xxx
LEFT JOIN
    dwh.fct_sale fs ON fs.xxx = flps.xxx
LEFT JOIN
    dwh.fct_sale fs2 ON fs2.xxx = flps.xxx
LEFT JOIN
    dwh_crm.dim_customer_personal_details dcpd ON dcpd.xxx = flps.xxx
LEFT JOIN
    dwh.fct_sale_online_postcode fsop ON fsop.xxx = flps.xxx
LEFT JOIN
    dwh.dim_employee de ON de.xxx = flps.xxx
LEFT JOIN
    dwh.dim_store ds1 ON ds1.xxx = flps.xxx
LEFT JOIN
    dwh.dim_store ds ON ds.xxx = flps.xxx
LEFT JOIN
    dwh.dim_item di ON di.xxx = flps.xxx
LEFT JOIN
    dwh.dim_employee de1 ON de1.xxx = flps.xxx
LEFT JOIN
    dwh.dim_store ds2 ON ds2.xxx = flps.xxx

-- Comments --;
COMMENT ON MATERIALIZED VIEW dwh.fct_loss_prevention_sale_v IS 'Description: xxx description.
Subject Area: xxx business/function.
Updated: xxx refresh frequency.
2023-06-02 - xxx - Created the materialized view.';

COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.date_key IS 'Unique ID for date.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.date IS 'Date of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.time_key IS 'Unique ID for time.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.time IS 'Time of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.source_system IS 'System from which the customer record originated from.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.sale_line_sid IS 'Unique identifier for the sale line in the source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.invoice_sid IS 'Unique identifier for the transaction in the source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.receipt_number IS 'Receipt number as recorded on the receipt.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.extended_price_lcy IS 'Total sale amount for line in the store''s currency.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.quantity IS 'Number of item''s sold or returned.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.receipt_timestamp IS 'Timestamp of the receipt in the store''s local time.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.original_invoice_sid IS 'Unique identifier for the transaction in the source system of the original purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.original_sale_line_sid IS 'Reference to the original sale line this return relates to.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.original_extended_price_lcy IS 'Total sale amount for line in the store''s currency of original purchase (for returns).';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.returned_greater_than_paid_flag IS 'True if the customer received a greater return amount than originally paid.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.original_receipt_timestamp IS 'Timestamp of the receipt in the store''s local time of original purchase (for returns).';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.return_in_return_period IS 'True if:reason code in (''CRF'',''CRR'',    ''CRO'', ''DIS'', ''DIT'') for damaged/reaction/incorrectreturned within 90    days for customersreturned within 30    days for employee purchases to any tender or 90 days to a gift card or    used in another purchaseNULL for non-returns and returns without an invoice of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.return_reason_code IS 'Reason code entered at POS for the item being returned.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.customer_key IS 'Unique ID for customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.customer_sid IS 'Unique customer identifier from source system.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.customer_state IS 'State of customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.billing_address_id IS 'Billing address ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.billing_address_postcode IS 'Postcode of billing address.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.shipping_address_id IS 'Shipping address ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.shipping_address_postcode IS 'Postcode of shipping address.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.is_employee_flag IS 'True if customer is a staff member.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.employee_id IS 'Staff employee ID.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.in_employment_period_flag IS 'True if transaction date between employee hire and termination dates.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.business_title IS 'Business title of employee.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.employee_type IS 'Contract type of employee.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.job_family_group IS '"Distribution Centre", "Support Centre" or "Retail".';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cost_centre IS 'Cost centre of employee customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.employee_ea_flag IS 'True if the customer is an employee and used Endless Aisle.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.order_type IS '"C&C" or "EA".';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.primary_store_key IS 'Store key of customer employee''s primary store.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.primary_store_code IS 'Code of primary store of employment of employee customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.primary_store_name IS 'Primary store name of employment of employee customer.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.store_key IS 'Unique ID for store.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.sale_channel IS '"Store" or "Online".';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.store_code IS 'Code of store of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.store_name IS 'Store name of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.store_state IS 'State of store of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.store_country IS 'Country of store of purchase.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.item_key IS 'Unique ID for item.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.item_code IS 'Item code.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.item_description IS 'Item description.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.category_name IS 'Item category.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.subcategory_name IS 'Item subcategory.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.pim_brand_name IS 'Product brand.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.pim_product_name IS 'Product name.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.pim_shade_name IS 'Product shade name.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cashier_id IS 'Employee ID that processed the transaction.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cashier_business_title IS 'Business title of the cashier.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cashier_cost_centre IS 'Cost centre of cashier.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cashier_primary_store_code IS 'Code of primary store of employment of cashier.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.cashier_primary_store_state IS 'State of primary store of employment of cashier.';
COMMENT ON COLUMN dwh.fct_loss_prevention_sale_v.order_fulfilment_location_code IS 'Collection store code.';

-- Permissions --;
ALTER MATERIALIZED VIEW dwh.fct_loss_prevention_sale_v OWNER TO role_master;
GRANT SELECT ON dwh.fct_loss_prevention_sale_v TO role_select_dwh;

-- Debugging lines --;
-- DROP MATERIALIZED VIEW IF EXISTS dwh.fct_loss_prevention_sale_v
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'dwh.fct_loss_prevention_sale';
-- DROP TABLE IF EXISTS dwh.fct_loss_prevention_sale;
