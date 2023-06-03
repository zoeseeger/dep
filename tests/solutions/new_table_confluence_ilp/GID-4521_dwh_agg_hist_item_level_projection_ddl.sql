/* ----------------------------------------------------------------- */
/*                      Create dwh schema table                      */
/* ----------------------------------------------------------------- */
CREATE TABLE dwh.agg_hist_item_level_projection (
    forecast_creation_week text NOT NULL
    , item_code text NOT NULL
    , week_start_date date NOT NULL
    , closing_stock_on_hand numeric NULL
    , constrained_demand numeric NULL
    , unconstrained_demand numeric NULL
    , projected_lost_sales numeric NULL
    , end_of_period_safety_stock numeric NULL
    , end_of_period_model_stock numeric NULL
    , end_of_period_in_transit numeric NULL
    , record_created timestamptz NOT NULL DEFAULT now()
    , CONSTRAINT agg_hist_item_level_projection_pk PRIMARY KEY (forecast_creation_week, item_code, week_start_date)
);

-- Comments --;
COMMENT ON TABLE dwh.agg_hist_item_level_projection IS 'Description: A historical table of inventory level projection aggregated to the level of week and item.
Subject Area: Supply Operations.
Updated: Sunday 10PM.
2023-06-01 - Zoe Seeger - Created the table.';

COMMENT ON COLUMN dwh.agg_hist_item_level_projection.forecast_creation_week IS 'Fiscal year, month, week projections created.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.item_code IS 'Item code.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.week_start_date IS 'The start date of the week that the values are projected for.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.closing_stock_on_hand IS 'The amount of stock the item-site combination is projected to end the week with on hand.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.constrained_demand IS 'The projected sales or tester consumption of the item at the location within the week, constrained by projected available stock on hand.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.unconstrained_demand IS 'The projected sales or tester consumption of the item at the location within the week.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.projected_lost_sales IS 'The projected sales or tester consumption of the item at the location within the week that are expected to not occur due to running out of stock.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.end_of_period_safety_stock IS 'The planned minimum stock level at the end of the week.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.end_of_period_model_stock IS 'The planned model stock at the end of the week (safety stock + replenishment cycle stock / 2).';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.end_of_period_in_transit IS 'The amount of stock the item-site combination is projected to end the week with in transit.';
COMMENT ON COLUMN dwh.agg_hist_item_level_projection.record_created IS 'Date record created.';

-- Table parameters --;
INSERT INTO adm.table_parameters(table_name, increment_from, overlap) values('dwh.agg_hist_item_level_projection', '1997-12-21', '0 minutes');

-- Permissions --;
ALTER TABLE dwh.agg_hist_item_level_projection OWNER to role_master;
GRANT SELECT ON dwh.agg_hist_item_level_projection TO role_select_dwh;

-- Debugging lines --;
-- DELETE FROM adm.table_parameters WHERE "table_name" = 'dwh.agg_hist_item_level_projection';
-- DROP TABLE IF EXISTS dwh.agg_hist_item_level_projection;
