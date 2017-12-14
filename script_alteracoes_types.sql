--select * from ma_stg_order_item_dist idist
--select * from  ordhead
drop type PoSearchLHBTbl;
drop type PoSearchLHBObj;
create or replace TYPE PoSearchLHBObj AS OBJECT(ord_level number,
                                             master_order_no number,
                                             order_no number,
                                             option_id varchar2(25),
                                             item_desc varchar2(4000),
                                             item_status_desc varchar2(4000),
                                             status varchar2(1),
                                             status_desc varchar2(250),
                                             supplier number,
                                             supplier_reference varchar2(30),
                                             supp_desc varchar2(240),
                                             expected_delivery_date date,
                                             created_by varchar2(30),
                                             create_datetime  date,  -- new lhb 10-30-17
                                             fc number,
                                             po_type varchar2(1),
                                             po_source varchar2(50),
                                             handover_date date,
                                             handover_end_date date,
                                             product_group number,
                                             product_group_desc varchar2(4000),
                                             category number,
                                             category_desc varchar2(4000),
                                             sub_category number,
                                             sub_category_desc varchar2(120),
                                             class_key varchar2(32),
                                             subclass_key varchar2(48),
                                             business_model varchar2(250),
                                             business_model_name varchar2(250),
                                             buying_group varchar2(250),
                                             buying_group_name varchar2(250),
                                             buying_group_key varchar2(32),
                                             buying_subgroup varchar2(250),
                                             buying_subgroup_name varchar2(250),
                                             buying_subgroup_key varchar2(48),
                                             buying_set varchar2(250),
                                             buying_set_name varchar2(250),
                                             buying_set_key varchar2(64),
                                             factory varchar2(10),
                                             units NUMBER(20,4)                                            
                                             );
create or replace TYPE PoSearchLHBTbl AS TABLE OF PoSearchLHBObj;

---   planning 
drop type PoPlannSearchLHBTbl;
create or replace TYPE PoPlannSearchLHBObj FORCE AS OBJECT(process_id number(10),
                                                        order_level number,
                                                        order_rec_no  number(12),
                                                        po_type varchar2(4),
                                                        option_id varchar2(25),
                                                        item_desc varchar2(250),
                                                        qty_ordered number(12,4),
                                                        supplier  number(10),
                                                        factory varchar2(10),
                                                        supp_desc varchar2(240),
                                                        first_dest number(10),
                                                        final_dest  number(10),
                                                        fc_name varchar2(150),
                                                        loc_type  varchar2(1),
                                                        size_profile  varchar2(10),
                                                        handover_date date,
                                                        not_before_date date,
                                                        not_after_date  date,
                                                        create_datetime date,
                                                        last_update_datetime  date,
                                                        create_id varchar2(30),
                                                        last_update_id  varchar2(30),
                                                        supp_ref  varchar2(30),
                                                        product_group number(4),
                                                        category  number(4),
                                                        sub_category  number(4),
                                                        business_model  varchar2(250),
                                                        buying_group  varchar2(250),
                                                        sku_id  varchar2(25),
                                                        size_code varchar2(10),
                                                        units NUMBER(20,4));
create or replace TYPE PoPlannSearchLHBTbl AS TABLE OF PoPlannSearchLHBObj;
-- REPLENISHMENT
drop TYPE PoReplenSearchLHBTbl AS TABLE OF PoReplenSearchLHBObj;
create or replace TYPE PoReplenSearchLHBObj AS OBJECT(order_level           number,
                                                   parent                varchar2(25),
                                                   item_desc             varchar2(250),
                                                   supp_ref              varchar2(30),
                                                   supplier              number(10),
                                                   supp_desc             varchar2(240),
                                                   item                  varchar2(25),
                                                   size_code             varchar2(10),
                                                   qty_ordered           number,                                                  
                                                   primary_repl_supplier number(10),
                                                   location              number(10),    -- final_destination                                           
                                                   fc_name               varchar2(150),
                                                   loc_type              varchar2(1),
                                                   need_date             date,     -- delivery_date                                                      
                                                   product_group         number(4),
                                                   category              number(4),
                                                   sub_category          number(4),
                                                   business_model        varchar2(250),
                                                   buying_group          varchar2(250),
                                                   units                 NUMBER(20,4) ); -- new lhb 10-30-17
create or replace TYPE PoReplenSearchLHBTbl AS TABLE OF PoReplenSearchLHBObj;


           
           



