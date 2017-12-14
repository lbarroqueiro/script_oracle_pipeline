select (i.dept||' / '||i.class||' / '||i.subclass)
product_group
category
sub_category
incluir create_date  em  ma_v_po_search 
recriar PoSearchLObj (PoSearchLObj(Tbl) para incluir supplier_reference  varchar2(30) e create_date date

planning duvida com relação as units 

repleshiment 
alterar a view MA_V_REPLENISHMENT para incluir final_destination 
alterar o tipo PoReplenSearchObj para incluir delivery_date  e units ??? 

ma_stg_order_drops_detail
 select * from MA_V_REPLENISHMENT i,
 select *   from MA_V_PLANNING i

from ma_v_item_master i



SELECT ord.master_order_no master_order_no,
                 ord.order_no order_no,
                 ord.item,
                 i.item_desc item_desc,
                 (select code_desc
                    from ma_v_code_detail
                   where code_type = 'MAIS'
                     and code      = i.status) item_status_desc,
                 ord.status status,
                 (select code_desc
                    from ma_v_code_detail
                   where code_type = 'ORST'
                     and code      = ord.status) status_desc,
                 ord.supplier supplier,
                 ord.supplier_reference,
                 sup.sup_name supp_desc,
                 ord.expected_delivery_date,
                 ord.created_by,
                 ord.create_datetime,
                 ord.fc,
                 ord.po_type,
                 ord.po_source,
                 ord.handover_date,
                 i.dept product_group,
                 d.dept_name product_group_desc,
                 i.class category,
                 c.class_name category_desc,
                 i.subclass sub_category,
                 s.sub_name sub_category_desc,
                 c.class_key class_key,
                 s.subclass_key subclass_key,
                 mb.business_model,
                 mb.buying_group,
                 mb.buying_subgroup,
                 mb.buying_set,
                 mb.business_model_name,
                 mb.buying_group_name,
                 mb.buying_subgroup_name,
                 mb.buying_set_name,
                 buying_group_key,
                 buying_subgroup_key,
                 buying_set_key,
                 factory
            FROM ma_v_sups sup,
                 ma_v_item_master i,
                 ma_v_dept d,
                 ma_v_class c,
                 ma_v_subclass s,
                 ma_v_buyerarchy mb,
                 (SELECT o.master_order_no,
                         dd.order_no,
                         oo.option_id item,
                         o.status status,
                         o.supplier supplier,
                         isup.supplier_reference,
                         dd.first_dest_date expected_delivery_date,
                         o.create_id created_by,
                         o.create_datetime,
                         idist.first_dest fc,
                         idist.po_type po_type,
                         NULL po_source,
                         idist.handover_date handover_date,
                         oo.factory
                    FROM ma_stg_order o,
                         ma_stg_order_option oo,
                         ma_stg_order_item_dist idist,
                         ma_stg_order_drops_detail dd,
                         ma_stg_item_sup isup
                   WHERE oo.master_order_no     = o.master_order_no
                     AND oo.master_order_no     = idist.master_order_no(+)
                     AND oo.option_id           = idist.option_id(+)
                     AND idist.master_order_no  = dd.master_order_no(+)
                     AND idist.option_id        = dd.option_id(+)
                     AND idist.first_dest       = dd.first_dest(+)
                     AND idist.final_dest       = dd.final_dest(+)
                     AND o.status               IN ('S','W')
                     and oo.option_id           = isup.item(+)
                     and o.supplier             = isup.supplier(+)
                  UNION ALL
                  SELECT DISTINCT
                         oh.master_po_no,
                         oh.order_no,
                         im.item_parent item,
                         oh.status,
                         oh.supplier,
                         isup.vpn supplier_reference,
                         oh.not_after_date,
                         oh.create_id,
                         oh.create_datetime,
                         ol.location,
                         oh.po_type,
                         NULL po_source,
                         oh.pickup_date,
                         oh.factory
                    FROM ordhead oh,
                         ordloc  ol,
                         ma_v_item_master im,
                         ma_v_item_supplier isup
                   WHERE oh.order_no = ol.order_no
                     AND oh.status   = 'A'
                     AND ol.item     = im.item
                     and ol.item     = isup.item
                     and oh.supplier = isup.supplier
               ) ord
         WHERE ord.supplier = sup.supplier
           AND ord.item     = i.item (+)
           AND i.dept       = d.dept
           AND i.dept       = c.dept
           AND i.class      = c.class
           AND i.dept       = s.dept
           AND i.class      = s.class
           AND i.subclass   = s.subclass
           AND mb.item      = i.item






























































































































































































































































