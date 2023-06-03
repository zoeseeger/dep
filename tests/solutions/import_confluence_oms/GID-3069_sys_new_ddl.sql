/* ----------------------------------------------------------------- */
/*                           Create schema                           */
/* ----------------------------------------------------------------- */
CREATE SCHEMA sys_new;
ALTER SCHEMA sys_new OWNER TO role_master;
COMMENT ON SCHEMA sys_new IS 'This is the schema for the Tester Order System (TOS) which resides in mecca_oms (Host:db005wp.mecca.com.au) in the dbo schema.';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA sys_new TO role_master;

CREATE ROLE role_select_sys_new WITH
    NOLOGIN
    NOSUPERUSER
    INHERIT
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

GRANT role_connect_glinda TO role_select_sys_new;
COMMENT ON ROLE role_select_sys_new IS 'Access role to all datasources in sys_new schema';
GRANT USAGE ON SCHEMA sys_new TO role_select_sys_new;

/* ----------------------------------------------------------------- */
/*                      Create tmp schema table                      */
/* ----------------------------------------------------------------- */
CREATE TABLE tmp.new_orderitemadjustmentreasons (
    order_item_adjustment_reason_id int4
    , reason text
);

-- Permissions --;
ALTER TABLE tmp.new_orderitemadjustmentreasons OWNER to role_master;

/* ----------------------------------------------------------------- */
/*                    Create sys_new schema table                    */
/* ----------------------------------------------------------------- */
CREATE TABLE sys_new.orderitemadjustmentreasons (
    order_item_adjustment_reason_id int4 NOT NULL
    , reason text NOT NULL
    , record_created timestamptz NOT NULL DEFAULT now()
    , record_updated timestamptz NOT NULL DEFAULT now()
    , record_deleted timestamptz NULL
    , CONSTRAINT orderitemadjustmentreasons_pk PRIMARY KEY (order_item_adjustment_reason_id)
);

-- Comments --;
COMMENT ON TABLE sys_new.orderitemadjustmentreasons IS 'Description: Order adjustment reasons for orderitems.
Subject Area: Supply Chain.
Updated: Sunday 10PM.
2023-06-02 - Zoe Seeger - Created the table.';

-- Permissions --;
ALTER TABLE sys_new.orderitemadjustmentreasons OWNER to role_master;
GRANT SELECT ON sys_new.orderitemadjustmentreasons TO role_select_sys_new;

/* ----------------------------------------------------------------- */
/*                      Create tmp schema table                      */
/* ----------------------------------------------------------------- */
CREATE TABLE tmp.new_orderitems (
    order_item_id uuid
    , order_id uuid
    , item_code text
    , description text
    , quantity int4
    , unit_price numeric
    , adjusted_quantity int4
    , order_item_adjustment_reason_id int4
);

-- Permissions --;
ALTER TABLE tmp.new_orderitems OWNER to role_master;

/* ----------------------------------------------------------------- */
/*                    Create sys_new schema table                    */
/* ----------------------------------------------------------------- */
CREATE TABLE sys_new.orderitems (
    order_item_id uuid NOT NULL
    , order_id uuid NOT NULL
    , item_code text NOT NULL
    , description text NULL
    , quantity int4 NOT NULL
    , unit_price numeric NOT NULL
    , adjusted_quantity int4 NULL
    , order_item_adjustment_reason_id int4 NULL
    , record_created timestamptz NOT NULL DEFAULT now()
    , record_updated timestamptz NOT NULL DEFAULT now()
    , CONSTRAINT orderitems_pk PRIMARY KEY (order_item_id)
    , CONSTRAINT order_item_adjustment_reason_id_pk FOREIGN KEY (order_item_adjustment_reason_id) REFERENCES xxx(order_item_adjustment_reason_id)
);

CREATE INDEX orderitems_order_id_ix ON sys_new.orderitems USING btree (order_id);
CREATE INDEX orderitems_item_code_ix ON sys_new.orderitems USING btree (item_code);
CREATE INDEX orderitems_record_updated_ix ON sys_new.orderitems USING btree (record_updated);

-- Comments --;
COMMENT ON TABLE sys_new.orderitems IS 'Description: Tester order items.
Subject Area: Supply Chain.
Updated: Sunday 10PM.
2023-06-02 - Zoe Seeger - Created the table.';

-- Table parameters --;
INSERT INTO adm.table_parameters(table_name, increment_from, overlap) values('sys_new.orderitems', '1997-12-21', '0 minutes');

-- Permissions --;
ALTER TABLE sys_new.orderitems OWNER to role_master;
GRANT SELECT ON sys_new.orderitems TO role_select_sys_new;

-- Debugging lines --;
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'sys_new.orderitems';
-- DROP TABLE IF EXISTS sys_new.orderitems;
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'tmp.new_orderitems';
-- DROP TABLE IF EXISTS tmp.new_orderitems;
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'sys_new.orderitemadjustmentreasons';
-- DROP TABLE IF EXISTS sys_new.orderitemadjustmentreasons;
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'tmp.new_orderitemadjustmentreasons';
-- DROP TABLE IF EXISTS tmp.new_orderitemadjustmentreasons;
