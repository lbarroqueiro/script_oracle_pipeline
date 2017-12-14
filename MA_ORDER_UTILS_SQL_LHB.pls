create or replace PACKAGE BODY MA_ORDER_UTILS_SQL_LHB AS

--------------------------------------------------------------------------------

TYPE ORDER_OPTION_TYPE IS TABLE OF MA_STG_ORDER_OPTION%ROWTYPE;

--------------------------------------------------------------------------------
FUNCTION GET_ORDER_SEQ
RETURN NUMBER IS
BEGIN
  RETURN order_sequence.nextval;
END;
--------------------------------------------------------------------------------

FUNCTION GET_ITEM_DIST_ID_SEQ
RETURN NUMBER IS
BEGIN
  RETURN ma_stg_order_item_dist_seq.nextval;
END;
--------------------------------------------------------------------------------
FUNCTION ORDER_LIST (I_master_order_no IN  ORDHEAD.MASTER_PO_NO%TYPE DEFAULT NULL)
RETURN MA_ORDER_LIST_TBL PIPELINED AS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.ORDER_LIST';
  L_error_message VARCHAR2(2000);
  --
  L_ResultTbl     MA_ORDER_LIST_TBL;
  --
  cursor C_ord is
    SELECT new ma_order_list_obj(order_no,
                                 destination_fc,
                                 exp_handover_date,
                                 exp_handover_window,
                                 exp_ship_date,
                                 exp_del_date_first_dest,
                                 exp_del_date_final_dest,
                                 qty_ordered,
                                 ship_qty,
                                 qty_received,
                                 status)
      FROM
           (SELECT OH.ORDER_NO ORDER_NO,
                   (SELECT OL.LOCATION FROM ORDLOC OL WHERE OL.ORDER_NO = OH.ORDER_NO GROUP BY OL.LOCATION) DESTINATION_FC,
                   OH.PICKUP_DATE EXP_HANDOVER_DATE, 
                   OH.EARLIEST_SHIP_DATE EXP_HANDOVER_WINDOW,
                   OH.NOT_BEFORE_DATE EXP_SHIP_DATE, 
                   OH.NOT_AFTER_DATE EXP_DEL_DATE_FIRST_DEST, 
                   CASE 
                     WHEN OH.STATUS='D' THEN
                         OH.NOT_AFTER_DATE
                     ELSE
                        (SELECT AD.IN_STORE_DATE FROM ALLOC_DETAIL AD WHERE AD.ALLOC_NO IN (SELECT AH.ALLOC_NO FROM ALLOC_HEADER AH WHERE AH.ORDER_NO = OH.ORDER_NO) GROUP BY AD.IN_STORE_DATE)
                     END EXP_DEL_DATE_FINAL_DEST,
                   (SELECT SUM(OL.QTY_ORDERED) FROM ORDLOC OL WHERE OL.ORDER_NO = OH.ORDER_NO GROUP BY ORDER_NO) QTY_ORDERED,
                   (SELECT SUM(S.QTY_EXPECTED) FROM SHIPSKU S WHERE S.SHIPMENT IN (SELECT SH.SHIPMENT FROM SHIPMENT SH WHERE SH.ORDER_NO = OH.ORDER_NO)) SHIP_QTY,
                   (SELECT SUM(AD.QTY_RECEIVED) FROM ALLOC_DETAIL AD WHERE AD.ALLOC_NO IN (SELECT AH.ALLOC_NO FROM ALLOC_HEADER AH WHERE AH.ORDER_NO = OH.ORDER_NO)) QTY_RECEIVED,
                   OH.STATUS STATUS
              FROM 
                   ORDHEAD OH
              WHERE OH.master_po_no = I_master_order_no
           );
  --
BEGIN
  --
  IF I_master_order_no IS NULL THEN
    --
    RETURN;
    --
  END IF;
  --
  --
  open C_ord;
  fetch C_ord bulk collect into L_ResultTbl;
  close C_ord;
  --
  for i in 1..L_ResultTbl.count loop
    --
    --
    pipe row(MA_ORDER_LIST_OBJ(L_ResultTbl(i).order_no,
                               L_ResultTbl(i).destination_fc,
                               L_ResultTbl(i).exp_handover_date,
                               L_ResultTbl(i).exp_handover_window,
                               L_ResultTbl(i).exp_ship_date,
                               L_ResultTbl(i).exp_del_date_first_dest,
                               L_ResultTbl(i).exp_del_date_final_dest,
                               L_ResultTbl(i).qty_ordered,
                               L_ResultTbl(i).ship_qty,
                               L_ResultTbl(i).qty_received,
                               L_ResultTbl(i).status));
  --
  end loop;
  --

  --
  RETURN;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => null,--MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_MA_ORDER_LIST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
END ORDER_LIST;
--------------------------------------------------------------------------------
FUNCTION GET_ORDHEAD_CFA_RMS(I_master_order_no  IN   MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE,
                             I_order_no         IN   MA_STG_ORDER_DROPS_DETAIL.ORDER_NO%TYPE,
                             I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2 IS
  --
  L_program          VARCHAR2(64)   := 'MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS';
  L_error_message    VARCHAR2(2000);
  L_sql              VARCHAR2(4000);
  L_cfa_value        VARCHAR2(2000) := NULL;
  L_storage_col_name CFA_ATTRIB.STORAGE_COL_NAME%TYPE;
  L_data_type        CFA_ATTRIB.DATA_TYPE%TYPE;
  L_group_id         CFA_ATTRIB.GROUP_ID%TYPE;
  L_custom_ext_table CFA_EXT_ENTITY.CUSTOM_EXT_TABLE%TYPE;
  L_cfa_type         MA_CFA_CONF.CFA_TYPE%TYPE;
  --
  CURSOR C_get_cfa_info IS
    SELECT att.storage_col_name,
           --att.data_type,
           --e.base_rms_table,
           e.custom_ext_table,
           att.group_id
      FROM ma_cfa_conf cfa,
           cfa_attrib  att,
           cfa_attrib_group cg,
           cfa_attrib_group_set gs,
           cfa_ext_entity e
     WHERE cfa.cfa_type = I_cfa_type
       AND att.group_id      = cfa.group_id
       AND att.view_col_name = cfa.cfa_type
       AND cg.group_id       = cfa.group_id
       AND gs.group_set_id   = cg.group_set_id
       AND e.ext_entity_id   = gs.ext_entity_id;
  --
BEGIN
  --
  --
  --
  OPEN C_get_cfa_info;
  FETCH C_get_cfa_info INTO L_storage_col_name,
                            L_custom_ext_table,
                            L_group_id;
  CLOSE C_get_cfa_info;
  --
  -- Get CFA Value
  --
  L_sql :='SELECT '||L_storage_col_name||' cfa_value '||
            'FROM '|| L_custom_ext_table ||' d '||
           'WHERE d.order_no = '||I_order_no||' AND d.group_id = '||L_group_id;
  --
  EXECUTE IMMEDIATE L_sql INTO L_cfa_value;
  --
  RETURN L_cfa_value;
  --
EXCEPTION
  --
  when OTHERS then
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_GET_ORDHEAD_CFA_RMS',
                                              I_aux_1             => I_master_order_no,
                                              I_aux_2             => I_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN NULL;
    --
END GET_ORDHEAD_CFA_RMS;
--------------------------------------------------------------------------------
FUNCTION GET_ORDSKU_CFA_RMS(I_master_order_no  IN   MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE,
                            I_order_no         IN   MA_STG_ORDER_DROPS_DETAIL.ORDER_NO%TYPE,
                            I_item             IN   MA_STG_SIZING_SKU.SKU%TYPE,
                            I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2 IS
  --
  L_program          VARCHAR2(64)   := 'MA_ORDER_UTILS_SQL.GET_ORDSKU_CFA_RMS';
  L_error_message    VARCHAR2(2000);
  L_sql              VARCHAR2(4000);
  L_cfa_value        VARCHAR2(2000) := NULL;
  L_storage_col_name CFA_ATTRIB.STORAGE_COL_NAME%TYPE;
  L_data_type        CFA_ATTRIB.DATA_TYPE%TYPE;
  L_group_id         CFA_ATTRIB.GROUP_ID%TYPE;
  L_custom_ext_table CFA_EXT_ENTITY.CUSTOM_EXT_TABLE%TYPE;
  L_cfa_type         MA_CFA_CONF.CFA_TYPE%TYPE;
  --
  CURSOR C_get_cfa_info IS
    SELECT att.storage_col_name,
           --att.data_type,
           --e.base_rms_table,
           e.custom_ext_table,
           att.group_id
      FROM ma_cfa_conf cfa,
           cfa_attrib  att,
           cfa_attrib_group cg,
           cfa_attrib_group_set gs,
           cfa_ext_entity e
     WHERE cfa.cfa_type      = I_cfa_type
       AND att.group_id      = cfa.group_id
       AND att.view_col_name = cfa.cfa_type
       AND cg.group_id       = cfa.group_id
       AND gs.group_set_id   = cg.group_set_id
       AND e.ext_entity_id   = gs.ext_entity_id;
  --
BEGIN
  --
  --
  --
  OPEN C_get_cfa_info;
  FETCH C_get_cfa_info INTO L_storage_col_name,
                            L_custom_ext_table,
                            L_group_id;
  CLOSE C_get_cfa_info;
  --
  -- Get CFA Value
  --
  L_sql :='SELECT '||L_storage_col_name||' cfa_value '||
            'FROM '|| L_custom_ext_table ||' d '||
           'WHERE d.order_no = '||I_order_no||' AND d.item = '''||I_item||''' AND d.group_id = '||L_group_id;
  --
  EXECUTE IMMEDIATE L_sql INTO L_cfa_value;
  --
  RETURN L_cfa_value;
  --
EXCEPTION
  --
  when OTHERS then
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_GET_ORDSKU_CFA_RMS',
                                              I_aux_1             => I_master_order_no,
                                              I_aux_2             => I_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN NULL;
    --
END GET_ORDSKU_CFA_RMS;
--------------------------------------------------------------------------------
FUNCTION GET_PARTNER_CFA_RMS(I_partner_id       IN   PARTNER_CFA_EXT.PARTNER_ID%TYPE,
                             I_cfa_type         IN   MA_CFA_CONF.CFA_TYPE%TYPE)
RETURN VARCHAR2 IS
  --
  L_program          VARCHAR2(64)   := 'MA_ORDER_UTILS_SQL.GET_PARTNER_CFA_RMS';
  L_error_message    VARCHAR2(2000);
  L_sql              VARCHAR2(4000);
  L_cfa_value        VARCHAR2(2000) := NULL;
  L_storage_col_name CFA_ATTRIB.STORAGE_COL_NAME%TYPE;
  L_data_type        CFA_ATTRIB.DATA_TYPE%TYPE;
  L_group_id         CFA_ATTRIB.GROUP_ID%TYPE;
  L_custom_ext_table CFA_EXT_ENTITY.CUSTOM_EXT_TABLE%TYPE;
  L_cfa_type         MA_CFA_CONF.CFA_TYPE%TYPE;
  --
  CURSOR C_get_cfa_info IS
    SELECT att.storage_col_name,
           e.custom_ext_table,
           att.group_id
      FROM ma_cfa_conf cfa,
           cfa_attrib  att,
           cfa_attrib_group cg,
           cfa_attrib_group_set gs,
           cfa_ext_entity e
     WHERE cfa.cfa_type = I_cfa_type
       AND att.group_id      = cfa.group_id
       AND att.view_col_name = cfa.cfa_type
       AND cg.group_id       = cfa.group_id
       AND gs.group_set_id   = cg.group_set_id
       AND e.ext_entity_id   = gs.ext_entity_id;
  --
BEGIN
  --
  --
  --
  OPEN C_get_cfa_info;
  FETCH C_get_cfa_info INTO L_storage_col_name,
                            L_custom_ext_table,
                            L_group_id;
  CLOSE C_get_cfa_info;
  --
  -- Get CFA Value
  --partner_type, partner_id, group_id
  L_sql :='SELECT '||L_storage_col_name||' cfa_value '||
            'FROM '|| L_custom_ext_table ||' d '||
           'WHERE d.partner_type = ''FA'' and d.partner_id = '''||I_partner_id||''' AND d.group_id = '||L_group_id;
  --
  EXECUTE IMMEDIATE L_sql INTO L_cfa_value;
  --
  RETURN L_cfa_value;
  --
EXCEPTION
  --
  when OTHERS then
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_GET_PARTNER_CFA_RMS',
                                              I_aux_1             => I_partner_id,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN NULL;
    --
END GET_PARTNER_CFA_RMS;
--------------------------------------------------------------------------------
FUNCTION IS_ROW_LOCKED (O_error_message OUT VARCHAR2,
                        I_rowid         IN  ROWID,
                        I_table_name    IN  VARCHAR2)
RETURN VARCHAR2 IS
  --
  PRAGMA AUTONOMOUS_TRANSACTION;
  --
  L_program  VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.IS_ROW_LOCKED';  
  L_can_lock NUMBER;
  --
BEGIN
  --
  EXECUTE IMMEDIATE 'Begin
                     Select 1 into :x from '
                     || I_table_name
                     || ' where rowid =:v_rowid for update nowait;
                     Exception
                        When Others Then
                          :x:=null;
                     END;'
  USING OUT L_can_lock, I_rowid;
  --
  -- release locked if acquired
  --
  ROLLBACK;
  --
  if L_can_lock = 1 then
    --
    RETURN 'N';
    --
  elsif L_can_lock is null then
    --
    RETURN 'Y';
    --
  end if;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_IS_ROW_LOCKED',
                                              I_aux_1             => I_rowid, 
                                              I_aux_2             => I_table_name,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN 'Y';
    --
  --
END IS_ROW_LOCKED;
--------------------------------------------------------------------------------
FUNCTION PO_SEARCH(I_po_nbr               IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE DEFAULT NULL,
                   I_order_no             IN  ORDHEAD.ORDER_NO%TYPE DEFAULT NULL,
                   I_option_id            IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                   I_po_status            IN  MA_STG_ORDER.STATUS%TYPE DEFAULT NULL,
                   I_supplier             IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                   I_exp_delivery_date    IN  VARCHAR2 DEFAULT NULL,
                   I_created_by           IN  MA_STG_ORDER.CREATE_ID%TYPE DEFAULT NULL,
                   I_fc                   IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                   I_po_type              IN  MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE DEFAULT NULL,
                   I_po_source            IN  VARCHAR2 DEFAULT NULL,
                   I_handover_date        IN  VARCHAR2 DEFAULT NULL,
                   I_dept_list            IN  VARCHAR2 DEFAULT NULL,
                   I_class_list           IN  VARCHAR2 DEFAULT NULL,
                   I_subclass_list        IN  VARCHAR2 DEFAULT NULL,
                   I_business_model       IN  MA_V_UDA_ITEM_FF.UDA_TEXT%TYPE DEFAULT NULL,
                   I_business_model_list  IN  VARCHAR2 DEFAULT NULL,
                   I_buying_group_key     IN  VARCHAR2 DEFAULT NULL,
                   I_buying_group_list    IN  VARCHAR2 DEFAULT NULL,
                   I_buying_subgroup_list IN  VARCHAR2 DEFAULT NULL,
                   I_buying_set_list      IN  VARCHAR2 DEFAULT NULL,
                   I_process_id           IN  VARCHAR2 DEFAULT NULL,
                   I_process_date         IN  DATE DEFAULT NULL,
                   I_execute              IN  VARCHAR2 DEFAULT 'N',
                   I_factory              IN  MA_STG_ORDER_OPTION.FACTORY%TYPE DEFAULT NULL
                   )
RETURN PoSearchTbl PIPELINED AS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.PO_SEARCH';
  L_error_message VARCHAR2(2000);
  L_PoSearchTbl   PoSearchTbl;
  L_PoResultTbl   PoSearchTbl;
  L_string_query  VARCHAR2(20000);
  L_sys_refcur    SYS_REFCURSOR;
  --
  cursor C_ord_levels is 
    SELECT new PoSearchObj(ord_level,
                           master_order_no,
                           order_no,
                           option_id,
                           item_desc,
                           item_status_desc,
                           status,
                           status_desc,
                           supplier,
                           supp_desc,
                           expected_delivery_date,
                           created_by,
                           fc,
                           po_type,
                           po_source,
                           handover_date,
                           handover_end_date,
                           product_group,
                           product_group_desc,
                           category,
                           category_desc,
                           sub_category,
                           sub_category_desc,
                           class_key,
                           subclass_key,
                           business_model,
                           business_model_name,
                           buying_group,
                           buying_group_name,
                           buying_group_key,
                           buying_subgroup,
                           buying_subgroup_name,
                           buying_subgroup_key,
                           buying_set,
                           buying_set_name,
                           buying_set_key,
                           factory,
                           units
                          )
      FROM
           (SELECT DISTINCT
                   1 ord_level,
                   o.master_order_no master_order_no,
                   null order_no,
                   null option_id,
                   null item_desc,
                   null item_status_desc,
                   o.status status,
                   o.status_desc status_desc,
                   o.supplier,
                   o.supp_desc,
                   null expected_delivery_date,
                   o.created_by created_by,
                   null fc,
                   null po_type,
                   NULL po_source,
                   NULL handover_date,
                   NULL handover_end_date,
                   null product_group,
                   null product_group_desc,
                   null category,
                   null category_desc,
                   null sub_category,
                   null sub_category_desc,
                   null class_key,
                   null subclass_key,
                   null business_model,
                   null business_model_name,
                   null buying_group,
                   null buying_group_name,
                   null buying_group_key,
                   null buying_subgroup,
                   null buying_subgroup_name,
                   null buying_subgroup_key,
                   null buying_set,
                   null buying_set_name,
                   null buying_set_key,
                   NULL factory,
                   NULL units
              FROM (select *
                      from TABLE(L_PoSearchTbl)) o
            UNION ALL
            SELECT DISTINCT
                   2 ord_level,
                   d.master_order_no master_order_no,
                   d.order_no order_no,
                   null option_id,
                   null item_desc,
                   null item_status_desc,
                   d.status status,
                   d.status_desc status_desc,
                   d.supplier supplier,
                   d.supp_desc supp_desc,
                   d.expected_delivery_date,
                   d.created_by,
                   d.fc,
                   null po_type,
                   null po_source,
                   handover_date,
                   handover_end_date,
                   null product_group,
                   null product_group_desc,
                   null category,
                   null category_desc,
                   null sub_category,
                   null sub_category_desc,
                   null class_key,
                   null subclass_key,
                   null business_model,
                   null business_model_name,
                   buying_group,
                   buying_group_name,
                   null buying_group_key,
                   null buying_subgroup,
                   null buying_subgroup_name,
                   null buying_subgroup_key,
                   null buying_set,
                   null buying_set_name,
                   null buying_set_key,
                   factory,
                   (SELECT SUM(qty_ordered)
                      FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS(d.master_order_no)) t
                     WHERE t.order_no = d.order_no
                   )units
              FROM (select *
                      from TABLE(L_PoSearchTbl)) d
          UNION ALL
          select 3,
                 master_order_no,
                 order_no,
                 option_id,
                 item_desc,
                 item_status_desc,
                 status,
                 status_desc,
                 supplier,
                 supp_desc,
                 expected_delivery_date,
                 created_by,
                 fc,
                 po_type,
                 po_source,
                 handover_date,
                 handover_end_date,
                 product_group,
                 product_group_desc,
                 category,
                 category_desc,
                 sub_category,
                 sub_category_desc,
                 class_key,
                 subclass_key,
                 business_model,
                 business_model_name,
                 buying_group,
                 buying_group_name,
                 buying_group_key,
                 buying_subgroup,
                 buying_subgroup_name,
                 buying_subgroup_key,
                 buying_set,
                 buying_set_name,
                 buying_set_key,
                 null factory,
                 NULL units
            from TABLE(L_PoSearchTbl));
  --
BEGIN
  --
  IF I_execute = 'N' THEN
    --
    RETURN;
    --
  END IF;
  --
  EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_DATE_FORMAT = ''DD-MM-YYYY''');
  --
  -- build query string
  --
  L_string_query := q'{with t_binds as
                        (select :1  bv_po_nbr,
                                :2  bv_option_id,
                                :3  bv_po_status,
                                :4  bv_supplier,
                                :5  bv_exp_delivery_date,
                                :6  bv_created_by,
                                :7  bv_fc,
                                :8  bv_po_type,
                                :9  bv_po_source,
                                :10 bv_handover_date,
                                :11 bv_dept_list,
                                :12 bv_class_list,
                                :13 bv_subclass_list,
                                :14 bv_business_model,
                                :15 bv_buying_group_key,
                                :16 bv_factory,
                                :17 bv_order_no,
                                :18 bv_business_model_list,
                                :19 bv_buying_group_list,
                                :20 bv_buying_subgroup_list,
                                :21 bv_buying_set_list
                           from dual)
                       select /*+ result_cache */ new PoSearchObj(null,
                                                                  master_order_no,
                                                                  order_no,
                                                                  option_id,
                                                                  item_desc,
                                                                  item_status_desc,
                                                                  status,
                                                                  status_desc,
                                                                  supplier,
                                                                  supp_desc,
                                                                  expected_delivery_date,
                                                                  created_by,
                                                                  fc,
                                                                  po_type,
                                                                  po_source,
                                                                  handover_date,
                                                                  handover_date + mso.handover_days,
                                                                  product_group,
                                                                  product_group_desc,
                                                                  category,
                                                                  category_desc,
                                                                  sub_category,
                                                                  sub_category_desc,
                                                                  class_key,
                                                                  subclass_key,
                                                                  business_model,
                                                                  business_model_name,
                                                                  buying_group,
                                                                  buying_group_name,
                                                                  buying_group_key,
                                                                  buying_subgroup,
                                                                  buying_subgroup_name,
                                                                  buying_subgroup_key,
                                                                  buying_set,
                                                                  buying_set_name,
                                                                  buying_set_key,
                                                                  factory,
                                                                  null
                                                                  )
                         from ma_v_po_search p,
                              t_binds b,
                              ma_system_options mso
                        where 1 = 1
                        }';
  --
  -- po_nbr
  --
  if I_po_nbr is not null then
    --
    L_string_query := L_string_query || q'{ and ((p.master_order_no= TO_NUMBER(b.bv_po_nbr)) or
                                                 (p.master_order_no= (select master_order_no
                                                                          from ma_stg_order_drops
                                                                         where order_no = TO_NUMBER(b.bv_po_nbr))) 
                                                )
                                          }';
    --
  end if;
  --
  --
  -- order_no
  --
  if I_order_no is not null then
    --
    L_string_query := L_string_query || q'{ and p.order_no = TO_NUMBER(b.bv_order_no)}';
    --
  end if;
  --
  --  
  -- option_id
  --
  if I_option_id is not null then
    --
    L_string_query := L_string_query || q'{ and p.option_id = TO_CHAR(b.bv_option_id)}';

    --
  end if;
  --
  -- po_status
  --
  if I_po_status is not null then
    --
    L_string_query := L_string_query || q'{ and p.status = b.bv_po_status}';

    --
  end if;
  --
  -- supplier_site
  --
  if I_supplier is not null then
    --
    L_string_query := L_string_query || q'{ and p.supplier = TO_NUMBER(b.bv_supplier)}';
    --
  end if;
  --
  -- I_exp_delivery_date
  --
  if I_exp_delivery_date is not null then
    --
    L_string_query := L_string_query || q'{ and trunc(p.expected_delivery_date) = trunc(to_date(b.bv_exp_delivery_date,'DD-MM-YYYY'))                                                     
                                          }';
    --
  end if;
  --
  -- created_by
  --
  if I_created_by is not null then
    --
    L_string_query := L_string_query || q'{ and p.created_by = bv_created_by }';
    --
  end if;
  --
  -- fc
  --
  if I_fc is not null then
    --
    L_string_query := L_string_query || q'{ and p.fc = TO_NUMBER(bv_fc) }';
    --
  end if;
  --
  -- po_type
  --
  if I_po_type is not null then
    --
    L_string_query := L_string_query || q'{ and p.po_type = bv_po_type }';
    --
  end if;
  --
  -- po_source
  --
  if I_po_source is not null then
    --
    L_string_query := L_string_query || q'{ and p.po_source = bv_po_source }';
    --
  end if;
  --
  -- handover_date
  --
  if I_handover_date is not null then
    --
    L_string_query := L_string_query || q'{ and trunc(p.handover_date) = trunc(to_date(b.bv_handover_date,'DD-MM-YYYY'))
                                          }';
    --
  end if;
  --
  -- dept,class,subclass
  --
  if I_subclass_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.product_group, p.category, p.sub_category) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) , 
                                                                                                         TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,5,4))),
                                                                                                         TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,9,4)))
                                                                                                    from table(convert_comma_list(b.bv_subclass_list)))                                                                                       
                                           }';
    --
  elsif I_class_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.product_group,p.category) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) , 
                                                                                        TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,5,4)))
                                                                                   from table(convert_comma_list(b.bv_class_list)))                                                                                                       
                                           }';
    --
  elsif I_dept_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.product_group) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) 
                                                                        from table(convert_comma_list(b.bv_dept_list)))

                                          }'; 
    --
  end if;
  --
  --
  IF I_business_model IS NOT NULL THEN
    --
    L_string_query := L_string_query || q'{ and p.business_model = TO_NUMBER(bv_business_model) }';
    --
  END IF;
  --
  --
  IF I_business_model_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.business_model) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) 
                                                                         from table(convert_comma_list(b.bv_business_model_list)))

                                          }'; 
    --
  END IF;
  --
  --
  IF I_buying_group_key IS NOT NULL THEN
    --
    L_string_query := L_string_query || q'{ and p.buying_group_key = bv_buying_group_key }';
    --
  END IF;
  --
  --
  IF I_buying_group_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.buying_group) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) 
                                                                       from table(convert_comma_list(b.bv_buying_group_list)))

                                          }'; 
    --
  END IF;
  --
  --
  IF I_buying_subgroup_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.buying_subgroup) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) 
                                                                          from table(convert_comma_list(b.bv_buying_subgroup_list)))

                                          }'; 
    --
  END IF;
  --
  --
  IF I_buying_set_list is not null then
    --
    L_string_query := L_string_query || q'{ and (p.buying_set) IN (select TO_NUMBER(TRIM(LEADING '0' FROM substr(column_value,1,4))) 
                                                                     from table(convert_comma_list(b.bv_buying_set_list)))

                                          }'; 
    --
  END IF;
  --
  --
  IF I_factory IS NOT NULL THEN
    --
    L_string_query := L_string_query || q'{ and (p.supplier in (select msf.supplier
                                                                  from ma_v_supplier_factory msf
                                                                 where msf.factory  = bv_factory) 
                                                 )
                                          }';
    --
  END IF;
  --
  --
  dbms_output.put_line(L_string_query);  
  --
  -- bulk query to table type
  --
  open L_sys_refcur for L_string_query using I_po_nbr,
                                             I_option_id,
                                             I_po_status,
                                             I_supplier,
                                             I_exp_delivery_date,
                                             I_created_by,
                                             I_fc,
                                             I_po_type,
                                             I_po_source,
                                             I_handover_date,
                                             I_dept_list,
                                             I_class_list,
                                             I_subclass_list,
                                             I_business_model,
                                             I_buying_group_key,
                                             I_factory,
                                             I_order_no,
                                             I_business_model_list,
                                             I_buying_group_list,
                                             I_buying_subgroup_list,
                                             I_buying_set_list;
  loop
    --
    fetch L_sys_refcur bulk collect into L_PoSearchTbl limit 1000;
    exit when L_PoSearchTbl.count = 0;
    --
    -- pipe data form collection
    --
    open C_ord_levels;
    fetch C_ord_levels bulk collect into L_PoResultTbl;
    close C_ord_levels; 
    --
    for i in 1..L_PoResultTbl.count loop
      --
      pipe row(PoSearchobj(L_PoResultTbl(i).ord_level,
                           L_PoResultTbl(i).master_order_no,
                           L_PoResultTbl(i).order_no,
                           L_PoResultTbl(i).option_id,
                           L_PoResultTbl(i).item_desc,
                           L_PoResultTbl(i).item_status_desc,
                           L_PoResultTbl(i).status,
                           L_PoResultTbl(i).status_desc,
                           L_PoResultTbl(i).supplier,
                           L_PoResultTbl(i).supp_desc,
                           L_PoResultTbl(i).expected_delivery_date,
                           L_PoResultTbl(i).created_by,
                           L_PoResultTbl(i).fc,
                           L_PoResultTbl(i).po_type,
                           L_PoResultTbl(i).po_source,
                           L_PoResultTbl(i).handover_date,
                           L_PoResultTbl(i).handover_end_date,
                           L_PoResultTbl(i).product_group,
                           L_PoResultTbl(i).product_group_desc,
                           L_PoResultTbl(i).category,
                           L_PoResultTbl(i).category_desc,
                           L_PoResultTbl(i).sub_category,
                           L_PoResultTbl(i).sub_category_desc,
                           L_PoResultTbl(i).class_key,
                           L_PoResultTbl(i).subclass_key,
                           L_PoResultTbl(i).business_model,
                           L_PoResultTbl(i).business_model_name,
                           L_PoResultTbl(i).buying_group,
                           L_PoResultTbl(i).buying_group_name,
                           L_PoResultTbl(i).buying_group_key,
                           L_PoResultTbl(i).buying_subgroup,
                           L_PoResultTbl(i).buying_subgroup_name,
                           L_PoResultTbl(i).buying_subgroup_key,
                           L_PoResultTbl(i).buying_set,
                           L_PoResultTbl(i).buying_set_name,
                           L_PoResultTbl(i).buying_set_key,
                           L_PoResultTbl(i).factory,
                           L_PoResultTbl(i).units
                           ));
    --
    end loop;
    --
  end loop;
  --
  RETURN;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_PO_SEARCH',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END PO_SEARCH;
--------------------------------------------------------------------------------
FUNCTION CHECK_ORDER_LOCKS(O_error_message   OUT VARCHAR2,
                           I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN VARCHAR2 IS
  --
  L_program              VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CHECK_ORDER_LOCKS';
  L_is_order_locked      VARCHAR2(1)  := 'N';
  L_order_rowid_by_table ROWID;
  L_string_query         VARCHAR2(1000);
  PROGRAM_ERROR          EXCEPTION;
  --
  cursor C_order_tables is
    select 'MA_STG_ORDER' table_name
      from dual
    union all
    select 'MA_STG_ORDER_OPTION' table_name
      from dual;
  --
BEGIN
  --
  -- check for item locks on item related tables
  --
  for tbl_rec in C_order_tables loop
    --
    L_string_query := 'SELECT ROWID FROM ' || tbl_rec.table_name 
                      || ' WHERE master_order_no= ''' || I_master_order_no|| '''';
    --
    BEGIN
      --
      EXECUTE IMMEDIATE L_string_query into L_order_rowid_by_table;
      --
    EXCEPTION
      --
      when OTHERS then
        --
        CONTINUE;
        --
      --
    END;
    --
    -- if rowid from user_objects equal to table rowid then is locked
    --
    L_is_order_locked := IS_ROW_LOCKED (O_error_message => O_error_message,
                                        I_rowid         => L_order_rowid_by_table,
                                        I_table_name    => tbl_rec.table_name);
    --
    if O_error_message is NOT NULL then
      --
      RETURN 'Y';
      --
    elsif L_is_order_locked = 'Y' then
      --
      RETURN L_is_order_locked;
      --
    end if; 
    --
  end loop;
  --
  RETURN L_is_order_locked;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_CHECK_ORDER_LOCKS',
                                              I_aux_1             => I_master_order_no, 
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN 'Y';
    --
  --
END CHECK_ORDER_LOCKS;
--------------------------------------------------------------------------------
FUNCTION GET_NEXT_ORDER_NBR (O_error_message OUT VARCHAR2)
RETURN NUMBER IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.GET_NEXT_ORDER_NBR';  
  L_next_order_number NUMBER := NULL;
  --
BEGIN
  --
  L_next_order_number := order_sequence.nextval;
  --
  RETURN L_next_order_number;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_IS_GET_NEXT_ORDER_NBR',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN L_next_order_number;
    --
  --
END GET_NEXT_ORDER_NBR;
--------------------------------------------------------------------------------
FUNCTION GET_FINAL_DEST_SHIP_METHOD (O_error_message          OUT VARCHAR2,
                                     I_first_dest             IN  MA_STG_ORDER_ITEM_DIST.FIRST_DEST%TYPE, 
                                     I_final_dest             IN  MA_STG_ORDER_ITEM_DIST.FINAL_DEST%TYPE,
                                     O_ship_method_final_dest OUT MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE)
RETURN BOOLEAN IS
  --
  L_program                VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.GET_FINAL_DEST_SHIP_METHOD';  
  --
  L_ship_method_final_dest MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE := NULL;
  L_default_ship_method    MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE := '30';
  --
  CURSOR C_get_info IS
    SELECT shipping_method 
      FROM ma_trnsp_transit_matrix
     WHERE shipping_point  = I_first_dest
       AND receiving_point = I_final_dest;
  --
BEGIN
  --
  -- Validate Parameters
  --
  IF I_first_dest IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  IF I_final_dest IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  -- Get information
  -- 
  OPEN C_get_info;
  FETCH C_get_info INTO L_ship_method_final_dest;
  CLOSE C_get_info;
  --
  IF L_ship_method_final_dest IS NULL THEN
    L_ship_method_final_dest := L_default_ship_method;
  END IF;
  --
  O_ship_method_final_dest := L_ship_method_final_dest;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_GET_FINAL_DEST_SHIP_METHOD',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END GET_FINAL_DEST_SHIP_METHOD;

--------------------------------------------------------------------------------
FUNCTION GET_DATE (O_error_message          OUT VARCHAR2,
                   I_factory                IN     MA_STG_ORDER_OPTION.FACTORY%TYPE,
                   I_po_type                IN     MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE,
                   I_ship_port              IN     MA_STG_ORDER_ITEM_DIST.SHIP_PORT%TYPE,
                   I_first_dest             IN     MA_STG_ORDER_ITEM_DIST.FIRST_DEST%TYPE, 
                   I_ship_method            IN     MA_STG_ORDER_ITEM_DIST.SHIP_METHOD%TYPE, 
                   I_freight_forwarder      IN     MA_STG_ORDER_ITEM_DIST.FREIGHT_FORWARD%TYPE, 
                   I_final_dest             IN     MA_STG_ORDER_ITEM_DIST.FINAL_DEST%TYPE, 
                   I_ship_method_final_dest IN     MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE,             
                   IO_handover_date         IN OUT MA_STG_ORDER_ITEM_DIST.HANDOVER_DATE%TYPE,
                   IO_ship_date             IN OUT MA_STG_ORDER_ITEM_DIST.SHIP_DATE%TYPE,
                   IO_not_before_date       IN OUT MA_STG_ORDER_ITEM_DIST.NOT_BEFORE_DATE%TYPE,
                   IO_not_after_date        IN OUT MA_STG_ORDER_ITEM_DIST.NOT_AFTER_DATE%TYPE,
                   IO_first_dest_date       IN OUT MA_STG_ORDER_ITEM_DIST.FIRST_DEST_DATE%TYPE,  
                   IO_final_dest_date       IN OUT MA_STG_ORDER_ITEM_DIST.FINAL_DEST_DATE%TYPE,
                   O_ex_factory_date        OUT    MA_STG_ORDER_ITEM_DIST.EX_FACTORY_DATE%TYPE,
                   O_week_no                OUT    MA_STG_ORDER_ITEM_DIST.WEEK_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.GET_DATE';  
  --
  L_cy_cut_off     MA_TRANSIT_MATRIX.CY_CUT_OFF%TYPE;
  L_dwell_days     MA_TRANSIT_MATRIX.ORIGIN_DWELL%TYPE; 
  L_total_days     MA_TRANSIT_MATRIX.TOTAL_DAYS%TYPE; 
  L_cut_off_days   NUMBER(1) :=0;
  L_days           MA_TRNSP_TRANSIT_MATRIX.DAYS%TYPE;
  L_original_lang  V$NLS_PARAMETERS.VALUE%TYPE;
  L_handover_days  MA_SYSTEM_OPTIONS.HANDOVER_DAYS%TYPE :=0;
  --
  CURSOR C_get_language IS
    SELECT value 
      FROM v$nls_parameters 
     WHERE parameter='NLS_LANGUAGE';
  --
  CURSOR C_get_ma_transit_info IS
    SELECT cy_cut_off,
           origin_dwell dwell_days,
           total_days  
      FROM ma_transit_matrix
     WHERE shipping_point    = I_ship_port
       AND receiving_point   = I_first_dest
       AND shipping_method   = I_ship_method
       AND freight_forwarder = I_freight_forwarder;
  --
  CURSOR C_get_ma_trnsp_transit_info IS
    SELECT days  
      FROM ma_trnsp_transit_matrix
     WHERE shipping_point    = I_first_dest
       AND receiving_point   = I_final_dest
       AND shipping_method   = I_ship_method_final_dest;
  --
  CURSOR C_get_handover_days IS
    SELECT handover_days
      FROM ma_system_options;
  --
BEGIN
  --
  -- Get Original Lang
  --
  OPEN C_get_language;
  FETCH C_get_language INTO L_original_lang;
  CLOSE C_get_language;
  --
  -- Change Language to American
  --
  EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_LANGUAGE = ''AMERICAN''');
  --
  -- Check the different scenarios 
  --
  --
  -- Get Exfactory Days
  --
  OPEN C_get_handover_days;
  FETCH C_get_handover_days INTO L_handover_days;
  CLOSE C_get_handover_days;
  --
  IF IO_handover_date IS NOT NULL AND IO_not_before_date IS NULL AND IO_not_after_date IS NULL THEN 
    --
    IO_not_before_date := IO_handover_date;
    IO_not_after_date  := IO_not_before_date + L_handover_days;
    --  
  ELSIF IO_handover_date IS NULL AND IO_not_before_date IS NOT NULL AND IO_not_after_date IS NULL THEN 
    --
    IO_handover_date := IO_not_before_date;
    IO_not_after_date  := IO_not_before_date + L_handover_days;
    --
  ELSIF IO_handover_date IS NULL AND IO_not_before_date IS NOT NULL AND IO_not_after_date IS NOT NULL THEN 
    --
    IF IO_not_after_date <= IO_not_before_date THEN
      --
      O_error_message := 'Not After Date cannot be less than or equal to Not Before Date.';
      RETURN FALSE;
      --
    END IF;
    --
    IO_handover_date := IO_not_before_date;
    --
  ELSIF IO_handover_date IS NOT NULL AND IO_not_before_date IS NOT NULL AND IO_not_after_date IS NOT NULL THEN 
    --
    IF NOT IO_handover_date BETWEEN IO_not_before_date AND IO_not_after_date THEN
      --
      O_error_message := 'Hand over date is not in between the Hand over window.';
      RETURN FALSE;
      --
    END IF;
    --
    IF IO_not_after_date <= IO_not_before_date THEN
      --
      O_error_message := 'Not After Date cannot be less than or equal to Not Before Date.';
      RETURN FALSE;
      --
    END IF;
    --    
  ELSIF (IO_first_dest_date IS NOT NULL OR IO_final_dest_date IS NOT NULL)
    AND IO_handover_date IS NULL AND IO_not_before_date IS NULL AND IO_not_after_date IS NULL THEN        
    --
      OPEN C_get_ma_trnsp_transit_info;
      FETCH C_get_ma_trnsp_transit_info INTO L_days;
      CLOSE C_get_ma_trnsp_transit_info;
    --
    IF IO_final_dest_date IS NOT NULL THEN
      --
      IF L_days IS NULL THEN
        IO_first_dest_date := IO_final_dest_date;
      ELSE
        IO_first_dest_date := IO_final_dest_date - L_days; 
      END IF;
      --
    ELSIF IO_first_dest_date IS NOT NULL THEN
      --
      IF L_days IS NULL THEN
        IO_final_dest_date := IO_first_dest_date; 
      ELSE 
        IO_final_dest_date := IO_first_dest_date + L_days;
      END IF;
      --
    END IF;
    --
    -- Calculate Shipment Date
    --
    OPEN C_get_ma_transit_info;
    FETCH C_get_ma_transit_info INTO L_cy_cut_off,
                                     L_dwell_days,
                                     L_total_days;
    CLOSE C_get_ma_transit_info;
    --
    IO_handover_date := IO_first_dest_date - L_total_days;
    --
    IF to_char(IO_handover_date,'DAY') <> L_cy_cut_off THEN
    --IF TO_CHAR(IO_handover_date, 'DAY', 'NLS_DATE_LANGUAGE=AMERICAN') <> L_cy_cut_off THEN
      --
      L_cut_off_days := NEXT_DAY(upper(IO_handover_date), L_cy_cut_off) - IO_handover_date;
      --
       IF L_cut_off_days = 7 THEN
         L_cut_off_days:=0;
       END IF;
      --
    END IF;
    --
    IO_handover_date := IO_handover_date - L_cut_off_days;
    --
    IO_not_before_date := IO_handover_date;
    IO_not_after_date  := IO_not_before_date + L_handover_days;
    --
    IO_ship_date := IO_handover_date + L_cut_off_days + L_dwell_days;
    --
    IF I_po_type = 'D' THEN
      --
      IO_final_dest_date := IO_first_dest_date;
      --
    ELSE
      --
      OPEN C_get_ma_trnsp_transit_info;
      FETCH C_get_ma_trnsp_transit_info INTO L_days;
      CLOSE C_get_ma_trnsp_transit_info;
      --
      IO_final_dest_date := IO_first_dest_date + L_days;
      --
    END IF;
    --
  END IF;
  --
  L_cut_off_days := 0;
  --
  IF IO_first_dest_date IS NULL AND IO_final_dest_date IS NULL THEN
    --
    OPEN C_get_ma_transit_info;
    FETCH C_get_ma_transit_info INTO L_cy_cut_off,
                                     L_dwell_days,
                                     L_total_days;
    CLOSE C_get_ma_transit_info;
    --
    IF to_char(IO_handover_date,'DAY') <> L_cy_cut_off THEN
      --
      L_cut_off_days := NEXT_DAY(IO_handover_date, L_cy_cut_off) - IO_handover_date;
      --
       IF L_cut_off_days = 7 THEN
         L_cut_off_days:=0;
       END IF;
      --
    END IF;
    --
    IO_ship_date := IO_handover_date + L_cut_off_days + L_dwell_days;
    --
    IO_first_dest_date := IO_ship_date + (L_total_days - L_dwell_days);
    --
    IF I_po_type = 'D' THEN
      --
      IO_final_dest_date := IO_first_dest_date;
      --
    ELSE
      --
      OPEN C_get_ma_trnsp_transit_info;
      FETCH C_get_ma_trnsp_transit_info INTO L_days;
      CLOSE C_get_ma_trnsp_transit_info;
      --
      IO_final_dest_date := IO_first_dest_date + L_days;
      --
    END IF;
    --
  END IF;
  --
  IF IO_final_dest_date IS NOT NULL THEN
    --
    O_week_no := TO_NUMBER(TO_CHAR(IO_final_dest_date,'IW'));
    --
  END IF;
  --
  -- Ex_factory_Date
  --
  O_ex_factory_date := IO_handover_date;
  --
  IF I_factory IS NOT NULL THEN
    --
    O_ex_factory_date := O_ex_factory_date - NVL(TO_NUMBER(MA_ORDER_UTILS_SQL.GET_PARTNER_CFA_RMS(I_partner_id => I_factory,
                                                                                                  I_cfa_type   => 'EX_FACTORY_DAYS')),0);
    --
  END IF;
  --  
  -- Change Language to Original Language
  --
  EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_LANGUAGE = '''||L_original_lang||'''');
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    -- Change Language to Original Language
    --
    EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_LANGUAGE = '''||L_original_lang||'''');
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_GET_DATE',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END GET_DATE;
--------------------------------------------------------------------------------
FUNCTION CHECK_QTY (O_error_message OUT VARCHAR2,
                    I_option_id     IN  MA_STG_ORDER_ITEM_DIST.OPTION_ID%TYPE,
                    I_supplier    IN  MA_STG_ORDER.SUPPLIER%TYPE,
                    I_qty_ordered   IN  MA_STG_ORDER_ITEM_DIST.QTY_ORDERED%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CHECK_QTY';  
  --
  L_min_order_qty     MA_V_ITEM_SUPPLIER_COUNTRY.MIN_ORDER_QTY%TYPE;
  L_supp_pack_size    MA_V_ITEM_SUPPLIER_COUNTRY.SUPP_PACK_SIZE%TYPE := NULL;
  --
  CURSOR C_check_qty IS
    SELECT NVL(mc.min_order_qty, 1) min_order_qty, 
           ext.number_11 supp_pack_size
      FROM item_supp_country_cfa_ext ext, 
           ma_asos.ma_v_item_supplier_country mc
     WHERE mc.item = I_option_id
       AND mc.supplier = I_supplier
       AND EXISTS (SELECT 1
                     FROM ma_asos.ma_v_option_supplier os
                    WHERE os.item = I_option_id
                      AND os.supplier = I_supplier
                      AND os.origin_country_id = mc.origin_country_id)
       AND ext.item = mc.item
       AND ext.supplier = mc.supplier
       AND ext.origin_country_id = mc.origin_country_id;
  --
BEGIN
  --
  -- Validate Parameters
  --
  IF I_option_id IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  IF I_supplier IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  IF I_qty_ordered IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  -- Get info from ma_v_item_supplier_country view
  --
  OPEN C_check_qty;
  FETCH C_check_qty INTO L_min_order_qty,
                         L_supp_pack_size;
  CLOSE C_check_qty;
  --
  IF L_supp_pack_size IS NULL THEN
    --
    O_error_message := 'NO_DATA_FOUND';
    --
    RETURN FALSE;
    --
  END IF;
  --
  -- Check if the entered qty is more than the minimum order qty set up at item/supplier level
  --
  IF NOT I_qty_ordered >= L_min_order_qty THEN
    -- 
    O_error_message := 'Entered quantity is not less then minimum order qty for item. The minimum order quantity is '||L_min_order_qty;
    --
    RETURN FALSE;
    --
  END IF;
  --
  -- Check If the qty is not a multiple of ?Order Multiple?
  --
  IF NOT MOD(I_qty_ordered * L_supp_pack_size, L_supp_pack_size) = 0 THEN
    --
    O_error_message := 'Entered quantity is not in multiple of order multiple value. The order multiple value is '||L_supp_pack_size;
    --
    RETURN FALSE;
    --
  END IF;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_CHECK_QTY',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CHECK_QTY;
--------------------------------------------------------------------------------
FUNCTION CREATE_ITEM_DIST (O_error_message    OUT VARCHAR2,
                           I_master_order_no  IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_ITEM_DIST';  
  --
BEGIN
  --
  DELETE ma_stg_order_item_dist md
   WHERE master_order_no = I_master_order_no
     AND option_id NOT IN (SELECT option_id 
                             FROM ma_stg_order_option mo
                            WHERE md.master_order_no = mo.master_order_no);
  --
  MERGE INTO ma_stg_order_item_dist md
  USING (SELECT master_order_no, 
                po_type, 
                option_id, 
                first_dest, 
                final_dest, 
                handover_date, 
                qty_ordered, 
                unit_cost, 
                ship_method, 
                ship_date, 
                first_dest_date, 
                not_before_date, 
                not_after_date, 
                final_dest_date, 
                freight_forward, 
                ship_port, 
                del_port,
                supplier_reference,
                ship_method_final_dest,
                create_datetime,
                last_update_datetime,
                create_id,
                last_update_id       
           FROM (SELECT mop.master_order_no      master_order_no,                                                
                        'D'                      po_type,
                        mop.option_id            option_id,
                        NULL                     first_dest,
                        mf.wh                    final_dest, 
                        NULL                     handover_date,
                        0                        qty_ordered,
                        mop.unit_cost            unit_cost,
                        nvl(ms.ship_method,'30') ship_method,
                        NULL                     ship_date,
                        NULL                     first_dest_date,
                        NULL                     not_before_date,
                        NULL                     not_after_date,
                        NULL                     final_dest_date,
                        ms.ff                    freight_forward,
                        ms.ship_port             ship_port,
                        ms.del_port              del_port,
                        mop.supplier_reference   supplier_reference,
                        nvl(ms.ship_method,'30') ship_method_final_dest,       
                        SYSDATE                  create_datetime,
                        SYSDATE                  last_update_datetime,
                        get_app_user             create_id,
                        get_app_user             last_update_id
                   FROM ma_stg_order_option mop,
                        ma_stg_order        mo,    
                        ma_v_ship           ms,
                        ma_v_wh             mf,
                        item_loc            il
                  WHERE mop.master_order_no = mo.master_order_no
                    AND ms.supplier         = mo.supplier
                    AND mo.master_order_no  = I_master_order_no
                    AND il.item             = mop.option_id
                    AND il.loc              = mf.wh
                    AND il.loc_type         = 'W'
                    AND il.status           = 'A'
                    AND mf.org_unit_id      IN (SELECT p.org_unit_id
                                                  FROM partner_org_unit p
                                                 WHERE p.partner      = ms.supplier
                                                   AND p.partner_type = 'U')
                    AND (
                         exists (select 1 
                                   from ma_stg_order_item_dist 
                                  where master_order_no = mop.master_order_no
                                    and option_id       = mop.option_id
                                    and final_dest      = mf.wh)
                         or
                         not exists (select 1 
                                       from ma_stg_order_item_dist 
                                      where master_order_no = mop.master_order_no
                                        and option_id       = mop.option_id)
                        )
                )
        ) ms
        ON (md.master_order_no = ms.master_order_no
            AND md.option_id   = ms.option_id
            AND md.final_dest  = ms.final_dest) 
  WHEN MATCHED THEN
    UPDATE SET unit_cost = ms.unit_cost 
  WHEN NOT MATCHED THEN
    INSERT (id_seq,
            master_order_no, 
            po_type, 
            option_id, 
            first_dest, 
            final_dest, 
            handover_date, 
            qty_ordered, 
            unit_cost, 
            ship_method, 
            ship_date, 
            first_dest_date, 
            not_before_date, 
            not_after_date, 
            final_dest_date, 
            freight_forward, 
            ship_port, 
            del_port,
            supplier_reference,
            ship_method_final_dest,
            create_datetime,
            last_update_datetime,
            create_id,
            last_update_id)
     VALUES(ma_stg_order_item_dist_seq.nextval,
            ms.master_order_no, 
            ms.po_type, 
            ms.option_id, 
            ms.first_dest, 
            ms.final_dest, 
            ms.handover_date, 
            ms.qty_ordered, 
            ms.unit_cost, 
            ms.ship_method, 
            ms.ship_date, 
            ms.first_dest_date, 
            ms.not_before_date, 
            ms.not_after_date, 
            ms.final_dest_date, 
            ms.freight_forward, 
            ms.ship_port, 
            ms.del_port,
            ms.supplier_reference,
            ms.ship_method_final_dest,
            ms.create_datetime,
            ms.last_update_datetime,
            ms.create_id,
            ms.last_update_id);
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CREATE_ITEM_DIST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_ITEM_DIST;
--------------------------------------------------------------------------------
FUNCTION CREATE_DROP_DIST (O_error_message   OUT VARCHAR2,
                           I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE,
                           I_option_drop     IN  VARCHAR2 DEFAULT 'Y',
                           I_option_id       IN  MA_STG_ORDER_DROPS_DETAIL.OPTION_ID%TYPE,
                           I_new_qty         IN  MA_STG_ORDER_DROPS_DETAIL.QTY_ORDERED%TYPE DEFAULT 0)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_DROP_DIST';
  --
BEGIN
  --
  IF I_new_qty <> 0 THEN
    --
    UPDATE ma_stg_order_option
      SET qty_ordered = I_new_qty
     WHERE master_order_no = I_master_order_no
       AND option_id       = I_option_id;
    --
  END IF;
  --
  DELETE ma_stg_order_drops
    WHERE master_order_no = I_master_order_no;

  DELETE ma_stg_order_drops_detail
    WHERE master_order_no = I_master_order_no;
  --
  -- Insert Detail
  --
  INSERT INTO ma_stg_order_drops_detail
                 (master_order_no,
                  order_no,
                  po_type,
                  option_id,
                  first_dest,
                  final_dest,
                  handover_date,
                  qty_ordered,
                  unit_cost,
                  ship_method,
                  ship_date,
                  first_dest_date,
                  not_before_date,
                  not_after_date,
                  final_dest_date,
                  freight_forward,
                  ship_port,
                  del_port,
                  supplier_reference,
                  ship_method_final_dest,
                  seq_no,
                  create_datetime,
                  last_update_datetime,
                  create_id,
                  last_update_id)
            WITH tab_grp1 AS
              (SELECT od.master_order_no,
                      DECODE(I_option_drop, 'Y', NULL, od.option_id) option_id,
                      od.first_dest,
                      SUM(DECODE(od.po_type,'D',1,0)) flag_d,
                      SUM(DECODE(od.po_type,'A',1,0)) flag_a,
                      SUM(DECODE(od.po_type,'S',1,0)) flag_s,
                      od.handover_date,
                      od.ship_method,
                      od.freight_forward,
                      DECODE(I_option_drop, 'Y', NULL, od.supplier_reference) supplier_reference,
                      op.factory
                 FROM ma_stg_order_item_dist od,
                      ma_stg_order_option    op
                WHERE od.master_order_no = I_master_order_no
                  AND op.master_order_no = od.master_order_no
                  AND op.option_id       = od.option_id
                  AND (od.qty_ordered IS NOT NULL OR od.qty_ordered >0)
                GROUP BY od.master_order_no,
                         DECODE(I_option_drop, 'Y', NULL, od.option_id),
                         od.first_dest,
                         od.handover_date,
                         od.ship_method,
                         od.freight_forward,
                         DECODE(I_option_drop, 'Y', NULL, od.supplier_reference),
                         op.factory
              )
            SELECT tab.master_order_no,
                   tab.order_no,
                   d.po_type,
                   d.option_id,
                   tab.first_dest,
                   d.final_dest,
                   d.handover_date,
                   d.qty_ordered qty_ordered,
                   d.unit_cost,
                   tab.ship_method,
                   d.ship_date,
                   d.first_dest_date,
                   d.not_before_date,
                   d.not_after_date,
                   d.final_dest_date,
                   tab.freight_forward,
                   d.ship_port,
                   d.del_port,
                   d.supplier_reference,
                   CASE
                     WHEN d.po_type NOT IN ('D') THEN
                       (SELECT delivery_method
                          FROM ma_transportation_matrix mm
                         WHERE mm.shipping_point = d.first_dest
                           AND mm.receiving_point = d.final_dest
                        )
                     ELSE
                       NULL
                   END ship_method_final_dest,
                   d.id_seq,
                   SYSDATE create_datetime,
                   SYSDATE last_update_datetime,
                   USER create_id,
                   USER last_update_id
              FROM (SELECT MA_ORDER_UTILS_SQL.GET_ORDER_SEQ order_no,
                           t.*
                      FROM (SELECT tab.master_order_no,
                                   DECODE(I_option_drop, 'Y', NULL, tab.option_id) option_id,
                                   tab.first_dest,
                                   MIN(tab.flag_d) flag_d,
                                   MIN(tab.flag_s) flag_s,
                                   MIN(tab.flag_a) flag_a,
                                   CASE
                                     WHEN d.po_type IN ('D','A') AND tab.flag_d >0 AND tab.flag_s >0 AND tab.flag_a >0 THEN
                                       'Z'
                                     ELSE
                                       'S'
                                   END po_type,
                                   tab.handover_date,
                                   tab.ship_method,
                                   tab.freight_forward,
                                   DECODE(I_option_drop, 'Y', NULL, tab.supplier_reference) supplier_reference,
                                   tab.factory
                              FROM tab_grp1 tab,
                                   ma_stg_order_item_dist d
                             WHERE d.master_order_no = tab.master_order_no
                               AND DECODE(I_option_drop, 'Y', 'Z', tab.option_id) = DECODE(I_option_drop, 'Y', 'Z', d.option_id)
                               AND d.first_dest      = tab.first_dest
                               AND d.handover_date   = tab.handover_date
                               AND d.ship_method     = tab.ship_method
                               AND d.freight_forward = tab.freight_forward
                               AND DECODE(I_option_drop, 'Y', 'Z', tab.supplier_reference) = DECODE(I_option_drop, 'Y', 'Z', d.supplier_reference)
                             GROUP BY tab.master_order_no,
                                     DECODE(I_option_drop, 'Y', NULL, tab.option_id),
                                     tab.first_dest,
                                     CASE
                                       WHEN d.po_type IN ('D','A') AND tab.flag_d >0 AND tab.flag_s >0 AND tab.flag_a >0 THEN
                                         'Z'
                                       ELSE
                                         'S'
                                     END,
                                     tab.handover_date,
                                     tab.ship_method,
                                     tab.freight_forward,
                                     DECODE(I_option_drop, 'Y', NULL, tab.supplier_reference),
                                     tab.factory
                           )t
                   )tab,
                   (SELECT d.master_order_no,
                           d.po_type,
                           /*DECODE(I_option_drop, 'Y', NULL, d.option_id) */d.option_id,
                           d.first_dest,
                           d.final_dest,
                           d.handover_date,
                           d.qty_ordered,
                           d.ship_method,
                           d.ship_date,
                           d.first_dest_date,
                           d.not_before_date,
                           d.not_after_date,
                           d.final_dest_date,
                           d.freight_forward,
                           d.ship_port,
                           d.del_port,
                           /*DECODE(I_option_drop, 'Y', NULL, supplier_ref) */d.supplier_reference,
                           d.unit_cost,
                           d.id_seq,
                           op.factory
                      FROM ma_stg_order_item_dist d,
                           ma_stg_order_option    op
                     WHERE d.master_order_no  = I_master_order_no
                       AND op.master_order_no = d.master_order_no
                       AND op.option_id       = d.option_id
                       AND (d.qty_ordered IS NOT NULL OR d.qty_ordered >0)
                    GROUP BY d.master_order_no,
                             d.po_type,
                             /*DECODE(I_option_drop, 'Y', NULL, d.option_id)*/ d.option_id,
                             d.first_dest,
                             d.final_dest,
                             d.handover_date,
                             d.qty_ordered,
                             d.ship_method,
                             d.ship_date,
                             d.first_dest_date,
                             d.not_before_date,
                             d.not_after_date,
                             d.final_dest_date,
                             d.freight_forward,
                             d.ship_port,
                             d.del_port,
                             /*DECODE(I_option_drop, 'Y', NULL, supplier_ref)*/ d.supplier_reference,
                             d.unit_cost,
                             d.id_seq,
                             op.factory
                   )d
             WHERE d.master_order_no = tab.master_order_no
               AND DECODE(I_option_drop, 'Y', 'Z', tab.option_id) = DECODE(I_option_drop, 'Y', 'Z', d.option_id)
               AND d.first_dest      = tab.first_dest
               AND d.handover_date   = tab.handover_date
               AND d.ship_method     = tab.ship_method
               AND CASE
                 WHEN d.po_type IN ('D','A') AND tab.flag_d >0 AND tab.flag_s >0 AND tab.flag_a >0 THEN
                   'Z'
                 ELSE
                   'S'
               END = tab.po_type
               AND d.freight_forward = tab.freight_forward
               AND DECODE(I_option_drop, 'Y', 'Z', tab.supplier_reference) = DECODE(I_option_drop, 'Y', 'Z', d.supplier_reference)
               AND d.factory = tab.factory
            ORDER BY order_no;
  --
  -- Insert Header
  --
  INSERT INTO ma_stg_order_drops
         (master_order_no,
          order_no,
          po_type,
          option_id,
          first_dest,
          final_dest,
          handover_date,
          qty_ordered,
          unit_cost,
          ship_method,
          ship_date,
          first_dest_date,
          not_before_date,
          not_after_date,
          final_dest_date,
          freight_forward,
          ship_port,
          del_port,
          supplier_reference,
          create_datetime,
          last_update_datetime,
          create_id,
          last_update_id)
        SELECT master_order_no,
               order_no,
               CASE
                 WHEN flag_s > 0 AND flag_d > 0 AND flag_a = 0 THEN
                   'S'
                 WHEN flag_s = 0 AND flag_d > 0 AND flag_a > 0 THEN
                   'A'
                 WHEN flag_s > 0 AND flag_d > 0 AND flag_a > 0 THEN
                   DECODE(po_type, 'A', 'A', 'S')
                 WHEN flag_s > 0 AND flag_d = 0 AND flag_a > 0 THEN
                   po_type
                 WHEN flag_s > 0 AND flag_d = 0 AND flag_a = 0 THEN
                   'S'
                 WHEN flag_s = 0 AND flag_d > 0 AND flag_a = 0 THEN
                   'D'
                 WHEN flag_s = 0 AND flag_d = 0 AND flag_a > 0 THEN
                   'A'
               END po_type,
               option_id,
               first_dest,
               NULL final_dest,
               handover_date,
               SUM(qty_ordered),
               NULL unit_cost,
               ship_method,
               ship_date,
               first_dest_date,
               not_before_date,
               not_after_date,
               NULL final_dest_date,
               freight_forward,
               NULL ship_port,
               NULL del_port,
               supplier_reference,
               SYSDATE create_datetime,
               SYSDATE last_update_datetime,
               USER create_id,
               USER last_update_id
          FROM (SELECT master_order_no,
                       order_no,
                       po_type,
                       SUM(DECODE(po_type, 'D', 1, 0)) OVER (PARTITION BY master_order_no, order_no) flag_d,
                       SUM(DECODE(po_type, 'A', 1, 0)) OVER (PARTITION BY master_order_no, order_no) flag_a,
                       SUM(DECODE(po_type, 'S', 1, 0)) OVER (PARTITION BY master_order_no, order_no) flag_s,
                       DECODE(I_option_drop, 'Y', NULL, option_id) option_id,
                       first_dest,
                       --NULL final_dest,
                       handover_date,
                       qty_ordered qty_ordered,
                       --NULL unit_cost,
                       ship_method,
                       ship_date,
                       first_dest_date,
                       not_before_date,
                       not_after_date,
                       --NULL final_dest_date,
                       freight_forward,
                       --NULL ship_port,
                       --NULL del_port,
                       DECODE(I_option_drop, 'Y', NULL, supplier_reference) supplier_reference
                       --SYSDATE create_datetime,
                       --SYSDATE last_update_datetime,
                       --USER create_id,
                       --USER last_update_id
                  FROM ma_stg_order_drops_detail d
                 WHERE master_order_no = I_master_order_no
                  AND (qty_ordered IS NOT NULL OR qty_ordered >0)
               )
        GROUP BY master_order_no,
                 order_no,
                 CASE
                   WHEN flag_s > 0 AND flag_d > 0 AND flag_a = 0 THEN
                     'S'
                   WHEN flag_s = 0 AND flag_d > 0 AND flag_a > 0 THEN
                     'A'
                   WHEN flag_s > 0 AND flag_d > 0 AND flag_a > 0 THEN
                     DECODE(po_type, 'A', 'A', 'S')
                   WHEN flag_s > 0 AND flag_d = 0 AND flag_a > 0 THEN
                     po_type
                   WHEN flag_s > 0 AND flag_d = 0 AND flag_a = 0 THEN
                     'S'
                   WHEN flag_s = 0 AND flag_d > 0 AND flag_a = 0 THEN
                     'D'
                   WHEN flag_s = 0 AND flag_d = 0 AND flag_a > 0 THEN
                     'A'
                 END,
                 option_id,
                 first_dest,
                 handover_date,
                 ship_method,
                 ship_date,
                 first_dest_date,
                 not_before_date,
                 not_after_date,
                 freight_forward,
                 supplier_reference;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CREATE_DROP_DIST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_DROP_DIST;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_DETAILS (O_error_message   OUT VARCHAR2,
                                I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_SIZING_DETAILS';
  --
  CURSOR C_ma_stg_sizing_option_dist IS
    SELECT seq_no,
           master_order_no,
           order_no,
           option_id,
           final_dest,
           exp_delivery_date,
           size_group,
           size_profile
      FROM ma_stg_sizing_option_dist
     WHERE master_order_no = I_master_order_no
       AND sizing_applied  = 'Y';
BEGIN
  -- 
  DELETE ma_stg_sizing_option_dist od
   WHERE od.master_order_no = I_master_order_no
     AND NOT EXISTS (SELECT 1
                       FROM ma_stg_order_drops_detail d
                      WHERE d.master_order_no = od.master_order_no
                        AND d.option_id       = od.option_id
                        AND d.final_dest      = od.final_dest
                        AND D.seq_no          = od.seq_no
                        -- AND d.final_dest_date = od.exp_delivery_date
                        -- AND d.order_no        = od.order_no
                    );
  --
  MERGE INTO ma_stg_sizing_option_dist s
  USING (SELECT od.master_order_no master_order_no,
                od.order_no, 
                od.option_id option_id,
                od.final_dest final_dest,
                od.final_dest_date exp_delivery_date,
                od.qty_ordered qty_ordered,
                --'N' sizing_applied,
                NULL distributed_by,
                im.diff_2 size_group,
                od.supplier_reference,
                od.seq_no                 
           FROM ma_stg_order_drops_detail od,
                ma_v_item_master im
          WHERE od.master_order_no  = I_master_order_no
            AND im.item              = od.option_id) d
  ON (s.master_order_no  = d.master_order_no AND
      s.option_id         = d.option_id       AND
      s.final_dest        = d.final_dest AND
      s.seq_no            = d.seq_no
      --s.exp_delivery_date = d.exp_delivery_date
      --s.qty_ordered = d.qty_ordered
      )
  WHEN MATCHED THEN
    UPDATE SET 
         qty_ordered    = d.qty_ordered,
         sizing_applied = decode(sign(s.qty_ordered-d.qty_ordered),0,s.sizing_applied,'N'),
         distributed_by = decode(s.sizing_applied,'Y','Q',NULL),
         order_no       = d.order_no,
         exp_delivery_date = d.exp_delivery_date          
  WHEN NOT MATCHED THEN
    INSERT (seq_no,
            master_order_no,
            order_no,
            option_id,
            final_dest,
            exp_delivery_date,
            qty_ordered,
            sizing_applied,
            size_group,
            distributed_by,
            supplier_reference,
            create_datetime,
            last_update_datetime,
            create_id,
            last_update_id)
    VALUES (d.seq_no,
            d.master_order_no,
            d.order_no,
            d.option_id,
            d.final_dest,
            d.exp_delivery_date,
            d.qty_ordered,
            'N',
            d.size_group,
            d.distributed_by,
            d.supplier_reference,
            SYSDATE,
            SYSDATE,
            get_app_user,
            get_app_user);
  --
  FOR C_rec IN C_ma_stg_sizing_option_dist LOOP
    --
    IF MA_ORDER_UTILS_SQL.CREATE_SIZING_SKU(O_error_message     => O_error_message,
                                            I_seq_no            => C_rec.seq_no,
                                            I_master_order_no   => C_rec.master_order_no,
                                            I_order_no          => C_rec.order_no,
                                            I_option_id         => C_rec.option_id,
                                            I_final_dest        => C_rec.final_dest,
                                            I_exp_delivery_date => C_rec.exp_delivery_date,
                                            I_size_group_id     => C_rec.size_group,
                                            I_profile_id        => C_rec.size_profile) = FALSE THEN
      --
      RETURN FALSE;
      --
    END IF;
    --
  END LOOP;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CREATE_SIZING_DETAILS',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_SIZING_DETAILS;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_SKU (O_error_message     OUT VARCHAR2,
                            I_seq_no            IN  MA_STG_SIZING_SKU.SEQ_NO%TYPE,
                            I_master_order_no   IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                            I_order_no          IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                            I_option_id         IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,
                            I_final_dest        IN  MA_STG_SIZING_SKU.FINAL_DEST%TYPE,
                            I_exp_delivery_date IN  MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE,
                            I_size_group_id     IN  MA_STG_ITEM_HEAD.DIFF_2_GROUP%TYPE,
                            I_profile_id        IN  MA_SIZE_PROFILE_DETAIL.SIZE_PROFILE%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_SIZING_SKU';  
  --
BEGIN
  -- 
  DELETE ma_stg_sizing_sku ms
    WHERE master_order_no = I_master_order_no
      AND NOT EXISTS (SELECT 1
                       FROM ma_stg_sizing_option_dist md
                      WHERE md.master_order_no = ms.master_order_no
                        AND md.option_id       = ms.option_id
                        AND md.final_dest      = ms.final_dest
                    );
  --                           
  MERGE INTO ma_stg_sizing_sku s
  USING (SELECT I_seq_no            seq_no,
                I_master_order_no   master_order_no,
                I_order_no          order_no,          
                tab.option_id       option_id,
                I_final_dest        final_dest,
                I_exp_delivery_date exp_delivery_date,
                tab.sku_id          sku,
                tab.size_code,
                tab.percentage * 100 percentage,
                NULL ratio,
                (SELECT qty_ordered  
                   FROM ma_stg_sizing_sku
                  WHERE seq_no            = I_seq_no
                    AND master_order_no   = I_master_order_no
                    AND order_no          = I_order_no
                    AND final_dest        = I_final_dest
                    AND exp_delivery_date = I_exp_delivery_date
                    AND option_id         = I_option_id
                    AND sku               = tab.sku_id
                    AND I_profile_id     IS NULL
                    AND I_size_group_id  IS NOT NULL 
                ) qty_ordered,
                SYSDATE create_datetime,
                SYSDATE last_update_datetime, 
                get_app_user create_id,
                get_app_user last_update_id
          FROM (SELECT diff_2               size_code,
                       to_char(item_parent) option_id, 
                       item sku_id,
                       null percentage   
                  FROM item_master
                 WHERE item_parent = I_option_id
                   AND I_profile_id IS NULL
                   AND I_size_group_id IS NOT NULL
                UNION ALL
                SELECT diff_id              size_code,
                       to_char(I_option_id) option_id,
                       null sku_id, 
                       null percentage
                  from ma_v_diff_group_detail mv
                 where diff_group_id = I_size_group_id
                   AND not exists (SELECT 1 
                                     FROM item_master im
                                   WHERE im.item_parent = I_option_id
                                    AND mv.diff_id      = im.diff_2
                                  )
                  AND I_profile_id IS NULL
                  AND I_size_group_id IS NOT NULL
                UNION ALL
                SELECT size_code            size_code, 
                       to_char(I_option_id) option_id, 
                       item sku_id, 
                       d.percentile percentage           
                   FROM ma_size_profile_detail d,
                        item_master im
                  WHERE im.item_parent(+) = I_option_id
                    AND im.diff_2(+)      = d.size_code
                    AND d.size_profile    = I_profile_id 
                    AND I_size_group_id IS NULL
                    AND I_profile_id IS NOT NULL) tab
        ) d
  ON (s.master_order_no     = d.master_order_no          
    AND s.option_id         = d.option_id         
    AND s.final_dest        = d.final_dest     
    AND s.size_code         = d.size_code
    AND s.seq_no            = d.seq_no
    -- AND s.order_no         = d.order_no
    -- AND trunc(s.exp_delivery_date) = trunc(d.exp_delivery_date)
      )  
  WHEN MATCHED THEN 
    UPDATE SET
          order_no   = d.order_no,
          exp_delivery_date = trunc(d.exp_delivery_date),
          percentage = CASE 
                         WHEN I_size_group_id IS NULL AND I_profile_id IS NOT NULL 
                         THEN d.percentage
                         ELSE NULL
                       END     
  WHEN NOT MATCHED THEN
    INSERT (seq_no,
            master_order_no, 
            order_no,
            option_id, 
            final_dest, 
            exp_delivery_date, 
            sku, 
            size_code, 
            percentage, 
            ratio, 
            qty_ordered, 
            create_datetime, 
            last_update_datetime, 
            create_id, 
            last_update_id)
     VALUES(d.seq_no,
            d.master_order_no, 
            d.order_no,
            d.option_id, 
            d.final_dest, 
            d.exp_delivery_date, 
            d.sku, 
            d.size_code, 
            d.percentage, 
            d.ratio, 
            d.qty_ordered, 
            d.create_datetime, 
            d.last_update_datetime, 
            d.create_id, 
            d.last_update_id);    
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CREATE_SIZING_SKU',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_SIZING_SKU;
--------------------------------------------------------------------------------
FUNCTION CREATE_SIZING_SKU_FC (O_error_message    OUT VARCHAR2,
                               I_master_order_no  IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                               I_order_no         IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                               I_option_id        IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,  
                               I_size_group       IN  MA_STG_SIZING_OPTION_DIST.SIZE_GROUP%TYPE,                                                         
                               I_final_dest       IN  MA_STG_SIZING_SKU.FINAL_DEST%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_SIZING_SKU_FC';  
  PROGRAM_ERROR       EXCEPTION;
  L_master_order_no   MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE;
  L_order_no          MA_STG_SIZING_SKU.ORDER_NO%TYPE;                                                        
  L_final_dest        MA_STG_SIZING_SKU.FINAL_DEST%TYPE;
  L_exp_delivery_date MA_STG_SIZING_OPTION_DIST.EXP_DELIVERY_DATE%TYPE;
  L_option_id         MA_STG_SIZING_OPTION_DIST.OPTION_ID%TYPE; 
  L_seq_no            MA_STG_SIZING_OPTION_DIST.SEQ_NO%TYPE; 
  L_diff              NUMBER;
  L_dummy             VARCHAR2(1);
  L_qty_ordered       MA_STG_SIZING_OPTION_DIST.QTY_ORDERED%TYPE;
  L_count_size        NUMBER; 
  L_size_profile      MA_STG_SIZING_OPTION_DIST.SIZE_PROFILE%TYPE;
  --  
  CURSOR C_get_size_profile IS
    SELECT size_profile
      FROM ma_stg_sizing_option_dist
     WHERE master_order_no  = I_master_order_no
       AND final_dest       = I_final_dest
       AND size_group       = I_size_group
       AND order_no         = I_order_no 
       AND option_id        = I_option_id;
  -- 
  CURSOR C_get_info IS
    SELECT *
      FROM ma_stg_sizing_option_dist
     WHERE master_order_no  = I_master_order_no
       AND final_dest       = I_final_dest
       AND size_group       = I_size_group
       AND ((order_no = I_order_no AND option_id <> I_option_id)
          OR order_no <> I_order_no);
  --
  CURSOR C_get_data IS
    SELECT '1' dummy
      FROM ma_stg_sizing_sku
     WHERE master_order_no  = L_master_order_no 
       AND order_no         = L_order_no
       AND final_dest       = L_final_dest
       AND option_id        = L_option_id;
  --
  CURSOR C_get_diff IS
    WITH tab_count_sf AS
       (SELECT size_code size_code_sf,
               COUNT(1) over (PARTITION BY sf.master_order_no, sf.order_no, sf.final_dest, option_id) count_sf
          FROM ma_stg_sizing_sku sf
         WHERE sf.master_order_no  = I_master_order_no 
           AND sf.order_no         = I_order_no
           AND sf.final_dest       = I_final_dest
           AND option_id           = I_option_id 
           AND sf.sku IS NOT NULL
           AND sf.qty_ordered IS NOT NULL
      ORDER BY size_code ASC
        ),
      tab_count_st AS
       (SELECT  size_code size_code_st--COUNT(1) count_st
          FROM ma_stg_sizing_sku st
         WHERE st.master_order_no  = I_master_order_no
           AND st.order_no         = L_order_no
           AND st.final_dest       = L_final_dest
           AND option_id           = L_option_id
           AND st.sku IS NOT NULL
       ORDER BY size_code ASC
       )/*,
      tab_count_diff AS
       (SELECT ABS(st.count_st - sf.count_sf) count_diff
          FROM tab_count_sf sf, 
               tab_count_st st
       )*/
    SELECT COUNT(1) over (PARTITION BY 1) diff,
           sf.count_sf
      FROM tab_count_sf sf,
           tab_count_st st
     WHERE sf.size_code_sf = st.size_code_st;
  --
BEGIN
   --   
   OPEN C_get_size_profile;
   FETCH C_get_size_profile INTO L_size_profile;
   CLOSE C_get_size_profile;
   --  
   IF L_size_profile IS NOT NULL THEN   
    --
    FOR C_rec IN C_get_info LOOP
      --
      L_dummy             := NULL;
      L_diff              := NULL; 
      L_master_order_no   := C_rec.master_order_no;
      L_order_no          := C_rec.order_no;
      L_final_dest        := C_rec.final_dest;
      L_exp_delivery_date := C_rec.exp_delivery_date;
      L_option_id         := C_rec.option_id;
      L_seq_no            := C_rec.seq_no;
      L_qty_ordered       := C_rec.qty_ordered;
      --      
        OPEN C_get_data;
        FETCH C_get_data INTO L_dummy;
        CLOSE C_get_data;
        --
        IF L_dummy IS NULL THEN
          --
          IF MA_ORDER_UTILS_SQL.CREATE_SIZING_SKU(O_error_message     => O_error_message,
                                                  I_seq_no            => L_seq_no, 
                                                  I_master_order_no   => L_master_order_no,
                                                  I_order_no          => L_order_no,
                                                  I_option_id         => L_option_id,
                                                  I_final_dest        => L_final_dest,
                                                  I_exp_delivery_date => L_exp_delivery_date,
                                                  I_size_group_id     => I_size_group,
                                                  I_profile_id        => NULL) = FALSE THEN

            RAISE PROGRAM_ERROR;
          END IF;
          --
        END IF;
        --
        OPEN C_get_diff;
        FETCH C_get_diff INTO L_diff, L_count_size;
        CLOSE C_get_diff;
        --
       IF L_diff = L_count_size THEN
          --
          MERGE INTO ma_stg_sizing_sku s
            USING (SELECT master_order_no,
                          final_dest,
                          size_code,
                          qty_ordered,
                          percentage 
                     FROM ma_stg_sizing_sku
                    WHERE master_order_no = I_master_order_no
                      AND order_no        = I_order_no
                      AND final_dest      = I_final_dest
                      AND option_id       = I_option_id) d
            ON (s.master_order_no   = d.master_order_no         
              AND s.final_dest      = d.final_dest
              AND s.size_code       = d.size_code
              AND s.order_no        = L_order_no
              AND (s.option_id       = L_option_id ))  
            WHEN MATCHED THEN
              --UPDATE SET qty_ordered = d.qty_ordered;
              UPDATE SET qty_ordered = L_qty_ordered * (d.percentage/100),
                         percentage  = d.percentage;
          --
          UPDATE ma_stg_sizing_option_dist
            SET distributed_by = 'Q',
                sizing_applied = 'Y',
                size_profile = L_size_profile
           WHERE master_order_no = L_master_order_no
             AND order_no        = L_order_no
             AND final_dest      = I_final_dest
             AND option_id       = L_option_id;
         --
        END IF;
      --
    END LOOP;
    --
    END IF;
  --     
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'CREATE_SIZING_SKU_FC',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_SIZING_SKU_FC;
--------------------------------------------------------------------------------
FUNCTION GENERATE_SKU (I_master_order_no IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                       I_option_id        IN  MA_STG_SIZING_SKU.OPTION_ID%TYPE,
                       I_size_code        IN  MA_STG_SIZING_SKU.SIZE_CODE%TYPE)
RETURN MA_STG_SIZING_SKU.SKU%TYPE IS
  --
  L_sku MA_STG_SIZING_SKU.SKU%TYPE;
  --
  CURSOR C_sequence IS
    SELECT MA_ITEMS_UTILS_SQL.GET_NEXT_ITEM()
      FROM dual;
  --
BEGIN
  --
  OPEN C_sequence;
  FETCH C_sequence INTO L_sku;
  CLOSE C_sequence;
  --
  UPDATE ma_stg_sizing_sku
    SET sku = L_sku
   WHERE master_order_no = I_master_order_no
     AND option_id       = I_option_id
     AND size_code       = I_size_code
     AND sku IS NULL;
  --
  RETURN L_sku;
  --
END GENERATE_SKU;                              
--------------------------------------------------------------------------------
FUNCTION PUB_ORDER_SIZING_SKUS(O_error_message    OUT VARCHAR2,
                               I_master_order_no IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.PUB_ORDER_SIZING_SKUS';
  L_status_code   VARCHAR2(1);
  L_message_type  MA_ITEM_MFQUEUE.MESSAGE_TYPE%TYPE;
  L_option_id     MA_STG_ITEM_HEAD.ITEM%TYPE;
  L_barcode_id    MA_STG_ITEM_BARCODE.REF_ITEM%TYPE;
  PROGRAM_ERROR   EXCEPTION;
  --
  cursor C_get_unpub_options is
    select distinct option_id
      from ma_stg_sizing_sku s
     where s.master_order_no = I_master_order_no
       and not exists (select 1
                         from item_master
                        where item = s.sku);
  --
  cursor C_get_unpub_skus is
    select distinct sku, 
           size_code
      from ma_stg_sizing_sku s
     where s.master_order_no = I_master_order_no
       and s.option_id        = L_option_id
       and s.sku       is not null
       and not exists (select 1
                         from item_master
                        where item = s.sku);
  --
BEGIN
  --
  for opt_rec in C_get_unpub_options loop
    --
    L_option_id := opt_rec.option_id;
    --
    -- load option from rms
    --
    if MA_ITEMS_UTILS_SQL.LOAD_STG_RMS_ITEM(O_error_message => O_error_message,
                                            I_item          => L_option_id,
                                            I_item_level    => 1) = FALSE then
      --
      RAISE PROGRAM_ERROR;
      --
    end if;
    --
    for sku_rec in C_get_unpub_skus loop
      --
      -- item size
      --
      insert into ma_stg_item_size
        (option_id, sku_id, "SIZE", create_datetime, last_update_datetime, create_id, last_update_id)
      values
        (L_option_id,sku_rec.sku , sku_rec.size_code, sysdate, sysdate, get_app_user, get_app_user);
      --
      -- item sup
      --
      insert into ma_stg_item_sup
        (item, supplier, origin_country_id, supplier_reference, unit_cost, currency, packaging_method, primary_supp_ind, primary_country_ind, min_order_qty, pack_size, supplier_colour, exp_gbp_price,units_per_carton, create_datetime, last_update_datetime, create_id, last_update_id)
      (select sku_rec.sku, 
              supplier, 
              origin_country_id, 
              supplier_reference, 
              unit_cost, currency, 
              packaging_method, 
              primary_supp_ind, 
              primary_country_ind,
              min_order_qty, 
              pack_size, 
              supplier_colour, 
              exp_gbp_price,
              units_per_carton, 
              create_datetime, 
              last_update_datetime, 
              create_id, 
              last_update_id
         from ma_stg_item_sup s
        where item  = L_option_id);   
      --
      -- barcode
      --
      if ITEM_ATTRIB_SQL.NEXT_EAN(O_error_message => O_error_message,
                                  O_ean13         => L_barcode_id) = FALSE then
        --
        RAISE PROGRAM_ERROR;                             
        --
      end if;        
      --                                               
      insert into ma_stg_item_barcode
        (ref_item, option_id, sku_id, "SIZE", number_type, format, prefix, primary_ref_ind, create_datetime, last_update_datetime, create_id, last_update_id)
      values
        (L_barcode_id, L_option_id, sku_rec.sku, sku_rec.size_code, 'EAN13', null, null, 'Y', sysdate, sysdate, get_app_user, get_app_user);
      --
      -- publish size
      --
      if MA_ITEMS_SQL.PUB_ITEM_MSG(O_status_code   => L_status_code,
                                   O_error_message => O_error_message,
                                   O_message_type  => L_message_type,
                                   I_item_id       => sku_rec.sku) = FALSE then
        --
        RAISE PROGRAM_ERROR;
        --
      end if;
      --
    end loop;
    --
    -- clean option and sizes from stagging
    --
    if MA_ITEMS_UTILS_SQL.DELETE_STG_ITEM_TABLES(O_error_message  => O_error_message,
                                                 I_item           => L_option_id,
                                                 I_item_level     => 1) = FALSE then
      --
      RAISE PROGRAM_ERROR;
      --
    end if;
    --
  end loop;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  when PROGRAM_ERROR then
    --
    ROLLBACK;
    RETURN FALSE;
    --
  --
  when OTHERS then
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_PUB_ORDER_SIZING_SKUS',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    ROLLBACK;
    RETURN FALSE;
    --
  --
END PUB_ORDER_SIZING_SKUS;
--------------------------------------------------------------------------------
FUNCTION CHECK_APPROVAL_LIMIT(O_error_message   OUT VARCHAR2,
                              I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE,
                              I_role            IN  MA_PO_APPROVAL_LIMIT_DETAIL.ROLE_ID%TYPE)
RETURN BOOLEAN IS 
  --
  L_program                 VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_OPTION_DETAILS_COST';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
  L_error_message           VARCHAR2(255);
  L_approval_limit          MA_PO_APPROVAL_LIMIT_HEAD.APPROVAL_LIMIT%TYPE;
  L_total_cost              MA_PO_APPROVAL_LIMIT_HEAD.APPROVAL_LIMIT%TYPE := 0;
  L_supp_currency           MA_STG_COST_OPTION_DETAIL.SUPPLIER_CURRENCY%TYPE;
  L_unit_cost               MA_STG_COST_OPTION_DETAIL.TOTAL_UNIT_COST%TYPE;  
  L_qty_ordered             MA_STG_COST_OPTION_DETAIL.QTY_ORDERED%TYPE;
  --
  CURSOR C_approval_limit IS
    SELECT ah.approval_limit
      FROM ma_po_approval_limit_head ah,
           ma_po_approval_limit_detail ad
     WHERE ad.role_id  = I_role
       AND ad.level_no = ah.level_no;
  --
  CURSOR C_cost_per_option IS
    SELECT cod.supplier_currency, 
           cod.unit_cost,
           cod.qty_ordered     
      FROM ma_stg_cost_option_detail cod
     WHERE cod.order_level    = 2 
      AND cod.master_order_no = I_master_order_no;
  --
  BEGIN
  --
  OPEN C_cost_per_option;
  LOOP
    --
    FETCH C_cost_per_option INTO L_supp_currency, L_unit_cost, L_qty_ordered;
    --
    EXIT WHEN C_cost_per_option%NOTFOUND;
    --
    IF L_supp_currency <> 'GBP' THEN
      --
      L_unit_cost := currency_sql.convert_value(i_cost_retail_ind => 'N',
                                                i_currency_out    => L_supp_currency,
                                                i_currency_in     => 'GBP',
                                                i_currency_value  => L_unit_cost);
      --
    END IF;
    --
    L_total_cost := L_total_cost + (L_unit_cost*L_qty_ordered);     
    --      
  END LOOP; 
  --
  CLOSE C_cost_per_option;
  --
  OPEN C_approval_limit;
  FETCH C_approval_limit INTO L_approval_limit;
  CLOSE C_approval_limit;
  --  
  IF L_approval_limit < L_total_cost THEN
    RETURN FALSE;
  END IF;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_CHECK_APPROVAL_LIMIT',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    ROLLBACK;
    RETURN FALSE;
    --
END CHECK_APPROVAL_LIMIT;
--------------------------------------------------------------------------------
PROCEDURE DEQUEUE_PO_MASS_MNT_CALLBACK (context  RAW,
                                        reginfo  SYS.AQ$_REG_INFO,
                                        descr    SYS.AQ$_DESCRIPTOR,
                                        payload  RAW,
                                        payloadl NUMBER) IS
  --
  L_program             VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.DEQUEUE_PO_MASS_MNT_CALLBACK';
  L_error_message       MA_LOGS.MSG_CODE%TYPE;
  L_dequeue_options     DBMS_AQ.DEQUEUE_OPTIONS_T;
  L_message_properties  DBMS_AQ.MESSAGE_PROPERTIES_T;
  L_message_handle      RAW(16);
  L_payload             MA_PO_MASS_MNT_PAYLOAD;
  --
BEGIN
  --
  L_dequeue_options.msgid         := descr.msg_id;
  L_dequeue_options.consumer_name := descr.consumer_name;
  L_dequeue_options.visibility    := DBMS_AQ.IMMEDIATE;
  --
  DBMS_AQ.DEQUEUE(queue_name         => descr.queue_name,
                  dequeue_options    => L_dequeue_options,
                  message_properties => L_message_properties,
                  payload            => L_payload,
                  msgid              => L_message_handle);
  --
  IF PRC_PO_MASS_MNT_QUEUE_MESSAGE (O_error_message   => L_error_message,
                                    I_master_order_no => L_payload.master_order_no,
                                    I_order_no        => L_payload.order_no,
                                    I_message_type    => L_payload.message_type,
                                    I_mass_mnt_user   => L_payload.mass_mnt_user) = FALSE THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_DEQUEUE_PO_MASS_MNT_CALLBACK',
                                              I_aux_1             => L_payload.master_order_no,
                                              I_aux_2             => L_payload.order_no,
                                              I_aux_3             => L_payload.message_type,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
  END IF;
  --
  --COMMIT;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_DEQUEUE_PO_MASS_MNT_CALLBACK',
                                              I_aux_1             => L_payload.master_order_no,
                                              I_aux_2             => L_payload.order_no,
                                              I_aux_3             => L_payload.message_type,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
END DEQUEUE_PO_MASS_MNT_CALLBACK;
--------------------------------------------------------------------------------
FUNCTION PRC_PO_MASS_MNT_QUEUE_MESSAGE(O_error_message   OUT VARCHAR2,
                                       I_master_order_no IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                                       I_order_no        IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                                       I_message_type    IN  MA_ORDER_MFQUEUE.MESSAGE_TYPE%TYPE,
                                       I_mass_mnt_user   IN  MA_STG_UPLOAD_PROCESS_LINE_IDS.CREATE_ID%TYPE)
RETURN BOOLEAN IS
  --
  L_program              VARCHAR2(64)  := 'MA_ORDER_UTILS_SQL.PRC_PO_MASS_MNT_QUEUE_MESSAGE';
  L_queue_rec            MA_ORDER_MFQUEUE%ROWTYPE := NULL;
  L_status_code          VARCHAR2(255) := 'P';
  L_error_message        VARCHAR2(255) := NULL;
  L_count_error          NUMBER        := 0;
  L_count_not_processed  NUMBER        := 0;
  L_notification_type    RAF_NOTIFICATION_TYPE_B.NOTIFICATION_TYPE_CODE%TYPE;
  L_notification_desc    RAF_NOTIFICATION.NOTIFICATION_DESC%TYPE;
  L_notification_context RAF_NOTIFICATION_CONTEXT.NOTIFICATION_CONTEXT%TYPE;
  L_launchable           RAF_NOTIFICATION.LAUNCHABLE%TYPE;
  --
BEGIN
  --
  -- Process Integration
  --
  L_queue_rec.master_order_no := I_master_order_no;
  L_queue_rec.order_no        := I_order_no;
  L_queue_rec.message_type    := I_message_type;
  L_queue_rec.pub_sync        := 'N';
  --
  IF MA_ORDERS_SQL.ADDTOQ(O_error_message  => O_error_message,
                          IO_queue_rec     => L_queue_rec) = FALSE THEN
    --
    RETURN FALSE;
    --
  END IF;
  --
  IF MA_ORDERS_SQL.PUB_ORDER_MSG(O_status_code      => L_status_code,
                                 O_error_message    => O_error_message,
                                 I_master_order_no  => I_master_order_no,
                                 I_sync_integration => TRUE) = FALSE THEN
    --
    RETURN FALSE;
    --
  END IF;
  --
  -- Insert Notification
  --
  /*
  IF L_count_not_processed = 0 THEN
    --
    IF L_count_error = 0 THEN
      --
      L_notification_type    := 'Item Upload Ended Successfully';
      L_notification_desc    := 'Processed successfully (Process ID - ' || I_process_seq || ')';
      L_notification_context := 'title=Item Search|url=/WEB-INF/com/asos/merchandising/microapp/item/view/flow/SearchItemFlow.xml#SearchItemFlow|uploadProcessId=' || to_char(I_process_seq);
      L_launchable           := 'Y';
      --
    ELSE
      --
      L_notification_type    := 'Item Upload Ended With Errors';
      L_notification_desc    := 'Processed With ' || L_count_error || ' Error(s) (Process ID - ' || I_process_seq || ')';
      L_notification_context := 'title=Item Search|url=/WEB-INF/com/asos/merchandising/microapp/item/view/flow/SearchItemFlow.xml#SearchItemFlow|uploadProcessId=' || to_char(I_process_seq);
      L_launchable           := 'Y';
      --
    END IF;
    --
    IF MA_NOTIFICATIONS_SQL.INSERT_NOTIFICATION(O_error_message        => O_error_message,
                                                I_notification_type    => L_notification_type,
                                                I_notification_desc    => L_notification_desc,
                                                I_notification_context => L_notification_context,
                                                I_launchable           => L_launchable,
                                                I_user                 => I_upload_user) = false then
      --
      RETURN FALSE;
      --
    END IF;
    --
  END IF;
  */
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_PRC_PO_MASS_MNT_QUEUE_MESSAGE',
                                              I_aux_1             => I_master_order_no,
                                              I_aux_2             => I_order_no,
                                              I_aux_3             => I_message_type,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END PRC_PO_MASS_MNT_QUEUE_MESSAGE;
--------------------------------------------------------------------------------
FUNCTION ENQUEUE_PO_MASS_MNT_PROCESS (O_error_message   OUT VARCHAR2,
                                      I_master_order_no IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                                      I_order_no        IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                                      I_message_type    IN  MA_ORDER_MFQUEUE.MESSAGE_TYPE%TYPE,
                                      I_mass_mnt_user   IN  MA_STG_UPLOAD_PROCESS_LINE_IDS.CREATE_ID%TYPE)
RETURN BOOLEAN IS
  --
  L_program              VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.ENQUEUE_PO_MASS_MNT_PROCESS';
  L_enqueue_options      DBMS_AQ.ENQUEUE_OPTIONS_T;
  L_message_properties   DBMS_AQ.MESSAGE_PROPERTIES_T;
  L_message_handle       RAW(16);
  L_payload              MA_PO_MASS_MNT_PAYLOAD;
  L_queue_name           VARCHAR2(100) := 'MA_PO_MASS_MNT_Q';
  --
BEGIN
  --
  L_payload := MA_PO_MASS_MNT_PAYLOAD(master_order_no => I_master_order_no,
                                      order_no        => I_order_no,
                                      message_type    => I_message_type,
                                      mass_mnt_user   => I_mass_mnt_user);
  --
  L_enqueue_options.visibility         := DBMS_AQ.ON_COMMIT;
  --
  DBMS_AQ.ENQUEUE(queue_name         => L_queue_name,
                  enqueue_options    => L_enqueue_options,
                  message_properties => L_message_properties,
                  payload            => L_payload,
                  msgid              => L_message_handle);
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_ENQUEUE_PO_MASS_MNT_PROCESS',
                                              I_aux_1             => I_master_order_no,
                                              I_aux_2             => I_order_no,
                                              I_aux_3             => I_message_type,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END ENQUEUE_PO_MASS_MNT_PROCESS;
--------------------------------------------------------------------------------
FUNCTION MASS_MAINTENANCE_UPDATE(O_error_message       OUT VARCHAR2,
                                 I_order_no_tbl        IN  MA_ORDER_NO_TBL,
                                 I_exp_handover_date   IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 I_handover_date_start IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 I_handover_date_end   IN  MA_STG_ORDER_DROPS.HANDOVER_DATE%TYPE DEFAULT NULL,
                                 O_order_list          OUT MA_ORDER_NO_TBL)
RETURN BOOLEAN IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.MASS_MAINTENANCE_UPDATE';
  --
  PROGRAM_ERROR   EXCEPTION;
  --
  L_queue_rec              MA_ORDER_MFQUEUE%ROWTYPE := NULL;
  L_ship_date              MA_STG_ORDER_ITEM_DIST.SHIP_DATE%TYPE;
  L_handover_date          MA_STG_ORDER_ITEM_DIST.HANDOVER_DATE%TYPE;
  L_first_dest_date        MA_STG_ORDER_ITEM_DIST.FIRST_DEST_DATE%TYPE;
  L_not_before_date        MA_STG_ORDER_ITEM_DIST.NOT_BEFORE_DATE%TYPE;
  L_not_after_date         MA_STG_ORDER_ITEM_DIST.NOT_AFTER_DATE%TYPE;
  L_final_dest_date        MA_STG_ORDER_ITEM_DIST.FINAL_DEST_DATE%TYPE;
  L_week_no                MA_STG_ORDER_ITEM_DIST.WEEK_NO%TYPE;
  L_ex_factory_date        MA_STG_ORDER_ITEM_DIST.EX_FACTORY_DATE%TYPE;
  L_exp_delivery_date      MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE;
  L_ship_port              MA_STG_ORDER_ITEM_DIST.SHIP_PORT%TYPE;
  L_del_port               MA_STG_ORDER_ITEM_DIST.DEL_PORT%TYPE;
  L_freight_forward        MA_STG_ORDER_ITEM_DIST.FREIGHT_FORWARD%TYPE;
  L_po_type                MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE := 'D';
  L_ship_method            MA_STG_ORDER_ITEM_DIST.SHIP_METHOD%TYPE;
  L_ship_method_final_dest MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE;
  L_handover_days          MA_SYSTEM_OPTIONS.HANDOVER_DAYS%TYPE :=0;
  L_factory                MA_STG_ORDER_OPTION.FACTORY%TYPE;
  L_final_dest             MA_STG_ORDER_DROPS.FINAL_DEST%TYPE;
  L_first_dest             MA_STG_ORDER_DROPS.FIRST_DEST%TYPE;
  L_master_order_no        MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE;
  L_order_no               MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_seq_no                 MA_STG_ORDER_DROPS_DETAIL.SEQ_NO%TYPE;
  L_check_shipment         BOOLEAN := FALSE;
  L_status_code            VARCHAR2(1);
  L_order_no_old           MA_STG_ORDER_DROPS.ORDER_NO%TYPE := NULL;
  --
  CURSOR C_get_master_order IS
    WITH tab_order AS
      (SELECT order_no
         FROM TABLE(I_order_no_tbl) 
      ),
    tab_master_order AS
      (SELECT DISTINCT
              oh.master_po_no master_order_no
         FROM ordhead   oh,
              tab_order t
        WHERE oh.order_no = t.order_no
      )
  SELECT DISTINCT
         master_order_no,
         'A' status
    FROM tab_master_order;
  --
  CURSOR C_get_order_detail IS
    SELECT DISTINCT
           d.master_order_no,
           d.order_no,
           d.po_type,
           o.status,
           op.factory,
           d.ship_port,
           d.first_dest,
           d.ship_method, 
           d.freight_forward,
           d.final_dest,
           d.ship_method_final_dest,
           d.seq_no
      FROM ma_stg_order_drops_detail d,
           ma_stg_order o,
           ma_stg_order_option op
     WHERE d.master_order_no  = o.master_order_no
       AND d.master_order_no  = op.master_order_no
       AND d.option_id        = op.option_id
       AND d.order_no IN (SELECT order_no
                            FROM TABLE(I_order_no_tbl) 
                         );
  --
  CURSOR C_get_handover_days IS
    SELECT handover_days 
      FROM ma_system_options;
  --
BEGIN
  --
  O_order_list := MA_ORDER_NO_TBL();
  L_queue_rec.order_no := NULL;
  --
  -- Load to stg
  --
  FOR C_rec IN C_get_master_order LOOP
    --
    L_master_order_no := C_rec.master_order_no;
    --
    IF LOAD_ORDER_TO_STG(O_error_message    => O_error_message,
                         I_get_type         => 'E',
                         IO_master_order_no => L_master_order_no) = FALSE THEN
      --
      RETURN FALSE;
      --
    END IF;
    --
  END LOOP;
  --
  COMMIT;
  --
  OPEN C_get_handover_days;
  FETCH C_get_handover_days INTO L_handover_days;
  CLOSE C_get_handover_days;
  --
  FOR C_rec IN C_get_order_detail LOOP
    --
    L_master_order_no        := C_rec.master_order_no;
    L_order_no               := C_rec.order_no;
    L_po_type                := C_rec.po_type;
    L_factory                := C_rec.factory;
    L_ship_port              := C_rec.ship_port;
    L_first_dest             := C_rec.first_dest;
    L_ship_method            := C_rec.ship_method;
    L_freight_forward        := C_rec.freight_forward;
    L_final_dest             := C_rec.final_dest;
    L_ship_method_final_dest := C_rec.ship_method_final_dest;
    L_seq_no                 := C_rec.seq_no;
    --
    -- Check if order has shipment
    --
    IF I_exp_handover_date IS NOT NULL OR I_handover_date_start IS NOT NULL THEN
      --
      IF ORDER_STATUS_SQL.CHECK_SHIPMENT(O_error_message => O_error_message,
                                         O_err_flag      => L_check_shipment,
                                         I_order_no      => L_order_no,
                                         I_item          => NULL,
                                         I_location      => NULL) = FALSE THEN
        --
        RETURN FALSE;
        --
      END IF;
      --
      IF L_check_shipment THEN
        --
        O_order_list.extend();
        O_order_list(O_order_list.last) := MA_ORDER_NO_OBJ(L_order_no, O_error_message);
        --
        CONTINUE;
        --
      END IF;
      --
    END IF;
    --
    IF I_exp_handover_date IS NOT NULL OR I_handover_date_start IS NOT NULL THEN
      --
      IF MA_ORDER_UTILS_SQL.CHECK_ORDER_LOCKS(O_error_message   => O_error_message,
                                              I_master_order_no => L_master_order_no) = 'Y' THEN
        --
        O_order_list.extend();
        O_order_list(O_order_list.last) := MA_ORDER_NO_OBJ(L_order_no, 'Order is locked!');
        --
        CONTINUE;
        --
      END IF;
      --
    END IF;
    --
    -- Get dates
    --
    IF I_exp_handover_date IS NOT NULL THEN
      --
      L_handover_date := I_exp_handover_date;
      --
    ELSIF I_exp_handover_date IS NULL AND I_handover_date_start IS NOT NULL THEN
      --
      L_handover_date := I_handover_date_start;
      --
    ELSIF I_exp_handover_date IS NULL AND I_handover_date_end IS NOT NULL THEN
      --
      L_handover_date := I_handover_date_end - L_handover_days;
      --
    ELSE
      --
      O_error_message := 'One of the dates shoud be populated.';
      RETURN FALSE;
      --
    END IF;
    --
    IF MA_ORDER_UTILS_SQL.GET_DATE (O_error_message,
                                    L_factory,
                                    L_po_type,
                                    L_ship_port,
                                    L_first_dest, 
                                    L_ship_method, 
                                    L_freight_forward, 
                                    L_final_dest, 
                                    L_ship_method_final_dest,             
                                    L_handover_date,
                                    L_ship_date,
                                    L_not_before_date,
                                    L_not_after_date,
                                    L_first_dest_date,  
                                    L_final_dest_date,
                                    L_ex_factory_date,
                                    L_week_no) = FALSE THEN
      --
      RETURN FALSE;
      --
    END IF;
    --
    -- Update stging tables
    --
    UPDATE ma_stg_order_drops
      SET handover_date   = L_handover_date,
          first_dest_date = L_first_dest_date,
          not_before_date = L_not_before_date,
          not_after_date  = L_not_after_date,
          ship_date       = L_ship_date
     WHERE master_order_no = L_master_order_no
       AND order_no        = L_order_no;
    --
    UPDATE ma_stg_order_drops_detail
      SET handover_date   = L_handover_date,
          first_dest_date = L_first_dest_date,
          not_before_date = L_not_before_date,
          not_after_date  = L_not_after_date,
          ship_date       = L_ship_date,
          final_dest_date = L_final_dest_date
    WHERE master_order_no = L_master_order_no
      AND order_no        = L_order_no
      AND po_type         = L_po_type;
    --
    UPDATE ma_stg_sizing_option_dist
      SET exp_delivery_date = L_final_dest_date
     WHERE master_order_no = L_master_order_no
       AND order_no        = L_order_no
       AND seq_no          = L_seq_no;
    --
    UPDATE ma_stg_sizing_sku
      SET exp_delivery_date = L_final_dest_date
     WHERE master_order_no = L_master_order_no
       AND order_no        = L_order_no
       AND seq_no          = L_seq_no;
    -- 
    --
    -- Send to queue altered info
    --
    IF C_rec.status = 'A' THEN
      --
      -- Enqueue the message
      --
      IF NVL(L_order_no_old,'-999') <> L_order_no OR L_order_no_old IS NULL THEN
        --
        IF MA_ORDER_UTILS_SQL.ENQUEUE_PO_MASS_MNT_PROCESS (O_error_message   => O_error_message,
                                                           I_master_order_no => L_master_order_no,
                                                           I_order_no        => L_order_no,
                                                           I_message_type    => MA_ORDERS_SQL.ORDER_MOD,
                                                           I_mass_mnt_user   => GET_APP_USER) = FALSE THEN
          --
          RETURN FALSE;
          --
        END IF;
        --
      END IF;
      --
      L_order_no_old := L_order_no;
      --
      /*L_queue_rec.order_no        := L_order_no;
      L_queue_rec.master_order_no := L_master_order_no;
      --L_queue_rec.message_type    := MA_ORDERS_SQL.ORDERDTL_MOD;
      L_queue_rec.message_type    := MA_ORDERS_SQL.ORDER_MOD;
      L_queue_rec.pub_sync        := 'Y';
      --
      IF MA_ORDERS_SQL.ADDTOQ(O_error_message  => O_error_message,
                              IO_queue_rec     => L_queue_rec) = FALSE THEN
        --
        RETURN FALSE;
        --
      END IF;
      --
      -- Call the publication
      --
      IF MA_ORDERS_SQL.PUB_ORDER_MSG(O_status_code      => L_status_code,
                                     O_error_message    => O_error_message,
                                     I_master_order_no  => L_master_order_no,
                                     I_sync_integration => FALSE) = FALSE THEN
        --
        RETURN FALSE;
        --
      END IF;
      --*/
    END IF;
    --
  END LOOP;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_MASS_MAINTENANCE_UPDATE',
                                              I_aux_1             => NULL,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    ROLLBACK;
    RETURN FALSE;
  --
END MASS_MAINTENANCE_UPDATE;
--------------------------------------------------------------------------------
FUNCTION UPDATE_APPROVE_ORDER(O_error_message    OUT VARCHAR2,
                              I_master_order_no  IN  MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE,
                              I_order_no         IN  MA_STG_ORDER_DROPS.ORDER_NO%TYPE,
                              I_option_id        IN  MA_STG_ORDER_DROPS.OPTION_ID%TYPE,
                              I_unit_cost        IN  MA_STG_ORDER_DROPS.UNIT_COST%TYPE,
                              I_factory          IN  MA_STG_ORDER_OPTION.FACTORY%TYPE)
RETURN BOOLEAN IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.UPDATE_APPROVE_ORDER';
  --
  L_queue_rec     MA_ORDER_MFQUEUE%ROWTYPE := NULL;
  PROGRAM_ERROR   EXCEPTION;
  --
  CURSOR C_get_items IS
    SELECT DISTINCT
           s.order_no,
           s.master_order_no,
           s.sku,
           MA_ORDERS_SQL.ORDERDTL_MOD message_type,
           I_unit_cost unit_cost
      FROM ma_stg_sizing_sku s
     WHERE s.master_order_no = I_master_order_no
       AND s.option_id       = I_option_id
       AND (s.order_no = I_order_no OR I_order_no IS NULL) 
       AND sku IS NOT NULL
       AND NVL(s.qty_ordered,0) > 0;
  --
BEGIN
  --
  -- Check if it's 
  --
  IF I_master_order_no IS NOT NULL 
    AND I_order_no IS NULL 
    AND I_option_id IS NOT NULL 
    AND I_unit_cost IS NOT NULL THEN
    --
    /*
    UPDATE ma_stg_order_option
      SET unit_cost = I_unit_cost
     WHERE master_order_no = I_master_order_no
       AND option_id       = I_option_id;
    */
    --
    UPDATE ma_stg_order_drops_detail
      SET unit_cost = I_unit_cost
     WHERE master_order_no = I_master_order_no
       AND option_id       = I_option_id;
    --
  ELSIF I_master_order_no IS NOT NULL 
    AND I_order_no IS NOT NULL 
    AND I_option_id IS NOT NULL 
    AND I_unit_cost IS NOT NULL THEN
    --
    /*
    UPDATE ma_stg_order_drops_detail
      SET unit_cost = I_unit_cost
     WHERE master_order_no = I_master_order_no
       AND order_no        = I_order_no
       AND option_id       = I_option_id;
    */
    NULL;
    --
  ELSIF I_master_order_no IS NOT NULL 
    AND I_order_no IS NULL 
    AND I_option_id IS NOT NULL 
    AND I_factory IS NOT NULL THEN
    --
    NULL;
    --
  ELSE
    --
    O_error_message := 'Bad parameters.'; 
    RETURN FALSE;
    --
  END IF;
  --
  -- Recalculation the costs
  --
  IF MA_ORDER_UTILS_SQL.CALC_COST_SUMMARY(O_error_message   => O_error_message, 
                                          I_master_order_no => I_master_order_no) = FALSE THEN
    --
    RAISE PROGRAM_ERROR;
    --
  END IF;
  --
  --
  --
  FOR C_rec IN C_get_items LOOP
    --
    L_queue_rec.order_no        := C_rec.order_no;
    L_queue_rec.master_order_no := C_rec.master_order_no;
    L_queue_rec.item            := C_rec.sku;
    L_queue_rec.message_type    := C_rec.message_type;
    L_queue_rec.unit_cost       := C_rec.unit_cost;
    --
    if MA_ORDERS_SQL.ADDTOQ(O_error_message  => O_error_message,
                            IO_queue_rec     => L_queue_rec) = FALSE THEN
      --
      RAISE PROGRAM_ERROR;
      --
    end if;
    --
  END LOOP; 
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_UPDATE_APPROVE_ORDER',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    ROLLBACK;
    RETURN FALSE;
  --
END UPDATE_APPROVE_ORDER;
--------------------------------------------------------------------------------
FUNCTION CALCULATE_SIZING_QTY (O_error_message     OUT VARCHAR2,
                               I_dist_by           IN  VARCHAR2,
                               I_master_order_no   IN  MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE,
                               I_order_no          IN  MA_STG_SIZING_SKU.ORDER_NO%TYPE,
                               I_option_id         IN  MA_STG_ORDER_ITEM_DIST.OPTION_ID%TYPE,
                               I_final_dest        IN  MA_STG_ORDER_DROPS.FINAL_DEST%TYPE,
                               I_exp_delivery_date IN  MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALCULATE_SIZING_QTY';  
  --
  --L_min_order_qty     MA_V_ITEM_SUPPLIER_COUNTRY.MIN_ORDER_QTY%TYPE;
  --L_supp_pack_size    MA_V_ITEM_SUPPLIER_COUNTRY.SUPP_PACK_SIZE%TYPE := NULL;
  --
  CURSOR C_info_qty IS
    SELECT master_order_no,
           order_no,
           seq_no,
           option_id,
           final_dest,
           exp_delivery_date,
           sku,
           percentage,
           new_qty,
           (SUM(new_qty) OVER (PARTITION BY master_order_no, option_id, final_dest, exp_delivery_date) - qty_ordered) delta
      FROM (SELECT od.master_order_no,
                   od.order_no,
                   od.seq_no,
                   od.option_id,
                   od.final_dest,
                   ms.exp_delivery_date,
                   od.qty_ordered,
                   ms.sku,
                   ms.percentage,
                   ms.ratio,
                   CASE
                     WHEN I_dist_by = 'P' THEN
                       ROUND((od.qty_ordered * (NVL(ms.percentage,0)/100)))
                     WHEN I_dist_by = 'R' THEN
                       ROUND((od.qty_ordered * (ms.ratio/SUM(ms.ratio) OVER (PARTITION BY ms.master_order_no, ms.option_id, ms.final_dest, ms.exp_delivery_date))))
                     WHEN I_dist_by = 'Q' THEN
                       ms.qty_ordered
                   END new_qty
              FROM ma_stg_sizing_option_dist od,
                   ma_stg_sizing_sku ms
             WHERE od.master_order_no   = I_master_order_no
               AND od.order_no          = I_order_no
               AND od.option_id         = I_option_id
               AND od.final_dest        = I_final_dest
               AND TRUNC(od.exp_delivery_date) = TRUNC(TO_DATE(I_exp_delivery_date,'DD-MM-YYYY'))  
               AND od.master_order_no  = ms.master_order_no
               AND od.option_id         = ms.option_id
               AND od.order_no          = ms.order_no
               AND od.final_dest        = ms.final_dest
               AND od.exp_delivery_date = ms.exp_delivery_date
               AND ms.sku IS NOT NULL
               AND (ms.percentage IS NOT NULL 
                 OR ms.ratio IS NOT NULL 
                 OR ms.qty_ordered IS NOT NULL)
           );    
  --
  TYPE TP_INFO_QTY IS TABLE OF C_info_qty%ROWTYPE INDEX BY PLS_INTEGER;
  L_rec_info_qty TP_INFO_QTY;
  --
BEGIN
  --
  -- Validate Parameters
  --
  IF I_dist_by IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  IF I_master_order_no IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  IF I_option_id IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --  
  IF I_final_dest IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --  
  IF I_exp_delivery_date IS NULL THEN
    --
    O_error_message := 'INV_PARAMETER';    
    RETURN FALSE;
    --
  END IF;
  --
  -- Get info values
  --
  OPEN C_info_qty;
  FETCH C_info_qty BULK COLLECT INTO L_rec_info_qty;
  CLOSE C_info_qty;
  --
  -- Update the new QTY
  --
  IF L_rec_info_qty.count <=0 THEN
    --
    RETURN FALSE;
    --
  END IF;
  --
  FORALL i IN 1..L_rec_info_qty.count
    UPDATE ma_stg_sizing_sku
      SET qty_ordered        = L_rec_info_qty(i).new_qty
     WHERE master_order_no   = L_rec_info_qty(i).master_order_no
       AND order_no          = L_rec_info_qty(1).order_no
       AND option_id         = L_rec_info_qty(i).option_id
       AND final_dest        = L_rec_info_qty(i).final_dest
       AND exp_delivery_date = L_rec_info_qty(i).exp_delivery_date
       AND sku               = L_rec_info_qty(i).sku;
  --
  UPDATE ma_stg_sizing_option_dist
    SET qty_ordered = qty_ordered + L_rec_info_qty(1).delta,
        distributed_by = 'Q'/*,
        sizing_applied = 'Y'*/
   WHERE master_order_no   = L_rec_info_qty(1).master_order_no
     AND order_no          = L_rec_info_qty(1).order_no
     AND option_id         = L_rec_info_qty(1).option_id
     AND final_dest        = L_rec_info_qty(1).final_dest
     AND exp_delivery_date = L_rec_info_qty(1).exp_delivery_date;
  --
  -- Check if SUM qty is different from parent tables
  --   
  IF L_rec_info_qty(1).delta <> 0 THEN
    --
    UPDATE ma_stg_order_item_dist
      SET qty_ordered = qty_ordered + L_rec_info_qty(1).delta
     WHERE master_order_no = L_rec_info_qty(1).master_order_no
       AND id_seq           = L_rec_info_qty(1).seq_no
       AND option_id        = L_rec_info_qty(1).option_id
       AND final_dest       = L_rec_info_qty(1).final_dest
       AND final_dest_date  = L_rec_info_qty(1).exp_delivery_date;
    --
    UPDATE ma_stg_order_option
      SET qty_ordered = qty_ordered + L_rec_info_qty(1).delta
     WHERE master_order_no  = L_rec_info_qty(1).master_order_no
       AND option_id         = L_rec_info_qty(1).option_id;
    --
  END IF;
  --  
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_CALCULATE_SIZING_QTY',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALCULATE_SIZING_QTY;
--------------------------------------------------------------------------------
FUNCTION DELETE_STG_ORDER_TABLES(O_error_message    OUT VARCHAR2,
                                 I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.DELETE_STG_ORDER_TABLES';  
  --
BEGIN
  --
  --DELETE FROM ma_po_history WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_cost_up_charge_detail WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_cost_drop_detail WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_cost_option_detail WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_cost_duty_detail WHERE master_order_no = I_master_order_no; 
  DELETE FROM ma_stg_cost_expense_detail WHERE master_order_no = I_master_order_no;

  DELETE FROM ma_stg_sizing_sku WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_sizing_option_dist WHERE master_order_no = I_master_order_no;

  DELETE FROM ma_stg_order_drops_detail WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_order_drops WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_order_item_dist WHERE master_order_no = I_master_order_no;
  DELETE FROM ma_stg_order_drop WHERE master_order_no = I_master_order_no;

  DELETE FROM ma_stg_order_option WHERE master_order_no = I_master_order_no;  
  DELETE FROM ma_stg_order WHERE master_order_no = I_master_order_no;  
  DELETE FROM ma_stg_order_rec WHERE master_order_no = I_master_order_no;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_DELETE_STG_ORDER_TABLES',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END DELETE_STG_ORDER_TABLES;
--------------------------------------------------------------------------------
FUNCTION CALC_OPTION_DETAILS_COST(O_error_message    OUT VARCHAR2,
                                  I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                  VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_OPTION_DETAILS_COST';  
  --
  FUNCTION_ERROR             EXCEPTION;
  --
  L_error_message            VARCHAR2(255);
  L_total_expense            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_expense_plan       MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_upcharge           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_upcharge_plan      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_duty               MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_duty_plan          MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_landed_cost        MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_landed_cost_plan   MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_plan_buy_margin          MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_exp_buy_margin           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_order_no                 MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_po_type                  MA_STG_ORDER_DROPS.PO_TYPE%TYPE;
  L_carton_fill_rate         MA_FREIGHT_MATRIX.CARTON_FILL_RATE%TYPE;
  L_per_count                ELC_COMP.PER_COUNT%TYPE;
  L_freight_currency         MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_freight_cost             MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_freight_cost_plan        MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_transportation_currency  MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_transportation_cost      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_transportation_cost_plan MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_re_processing_cost       MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_re_processing_cost_plan  MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_labelon_cost             MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_labelon_cost_plan        MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_hndlcost_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_hndlcost_cost_plan       MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_supplier                 MA_V_SUPS.SUPPLIER%TYPE;
  L_option_id                MA_STG_ORDER_OPTION.OPTION_ID%TYPE;
  L_manu_country_id          MA_STG_ORDER_OPTION.MANU_COUNTRY_ID%TYPE;
  L_first_dest               MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_final_dest               MA_STG_ORDER_DROPS_DETAIL.FINAL_DEST%TYPE;
  L_ship_method              MA_STG_ORDER_DROPS_DETAIL.SHIP_METHOD%TYPE;
  L_hndlcost_rate            MA_HNDLCOST_MATRIX.RATE%TYPE;
  L_hndlcost_rate_plan       MA_HNDLCOST_MATRIX.RATE%TYPE;
  L_hndlcost_currency        MA_HNDLCOST_MATRIX.CURRENCY%TYPE;
  L_vat_region               VAT_REGION.VAT_REGION%TYPE;
  L_vat_code                 VAT_CODE_RATES.VAT_CODE%TYPE;
  L_vat_rate                 VAT_CODE_RATES.VAT_RATE%TYPE;
  L_retail_price             MA_V_ITEM_LOC.UNIT_RETAIL%TYPE;
  L_buy_value                MA_V_ITEM_LOC.UNIT_RETAIL%TYPE;
  L_terms                    MA_V_OPTION_SUPPLIER.FREIGHT_TERMS%TYPE;
  L_key_value_1              EXP_PROF_HEAD.KEY_VALUE_1%TYPE;
  L_key_value_2              EXP_PROF_HEAD.KEY_VALUE_2%TYPE;
  L_module                   EXP_PROF_HEAD.MODULE%TYPE;
  L_partner_1                SUP_IMPORT_ATTR.PARTNER_1%TYPE;
  L_partner_2                SUP_IMPORT_ATTR.PARTNER_2%TYPE;
  L_freight_forward          MA_STG_ORDER_DROPS.FREIGHT_FORWARD%TYPE;
  L_import_country           ADDR.COUNTRY_ID%TYPE;
  L_first_dest_date          MA_STG_ORDER_DROPS.FIRST_DEST_DATE%TYPE;
  L_commodity                ITEM_HTS.HTS%TYPE;
  L_rate                     EXP_PROF_DETAIL.COMP_RATE%TYPE;
  L_rate_plan                EXP_PROF_DETAIL.COMP_RATE%TYPE;
  L_av_rate                  HTS_TARIFF_TREATMENT.AV_RATE%TYPE;
  L_final_dest_date          MA_STG_ORDER_DROPS_DETAIL.FINAL_DEST_DATE%TYPE;
  L_cost_component           MA_TRANSPORTATION_MATRIX.COST_COMPONENT%TYPE;
  L_cost_currency            EXP_PROF_DETAIL.COMP_CURRENCY%TYPE;
  L_exchange_rate            CURRENCY_RATES.EXCHANGE_RATE%TYPE;   
  --
  CURSOR C_import_country IS
    SELECT country_id 
      FROM addr 
     WHERE module      = 'SUPP' 
       AND key_value_1 = L_supplier
       AND addr_type   = '01';
  --  
  CURSOR C_commodity IS
    SELECT hts commodity_code
      FROM item_hts
     WHERE item = L_option_id
       AND import_country_id = L_import_country
       AND origin_country_id = L_manu_country_id
       AND L_final_dest_date BETWEEN effect_from AND effect_to;
  --
  CURSOR C_hts_tariff_treatment IS
    SELECT av_rate 
      FROM hts_tariff_treatment
     WHERE hts = L_commodity
       AND import_country_id = L_import_country
       AND L_first_dest_date BETWEEN effect_from AND effect_to
       /*AND tariff_treatment = tariff_treatment*/; 
  --
  CURSOR C_terms IS
    SELECT code_desc 
      FROM (select distinct
             supplier,
             item,
             freight_terms
             from ma_v_option_supplier),
           code_detail 
     WHERE supplier  = L_supplier 
       AND item      = L_option_id
       AND code_type = 'MSHT'
       AND code      = freight_terms;
  --
  CURSOR C_sup_imp_attr IS
    SELECT NVL(s.partner_1,'Z999') partner_1,
           s.partner_2
      FROM sup_import_attr s
     WHERE s.supplier = L_supplier;
  --  
  CURSOR C_exp_prof IS
    SELECT d.comp_id,
           d.comp_rate,
           d.comp_currency  
      FROM exp_prof_head h,
           exp_prof_detail d 
     WHERE h.exp_prof_key  = d.exp_prof_key
       AND (h.key_value_1  = L_key_value_1 OR L_key_value_1 IS NULL)
       AND (h.key_value_2  = L_key_value_2 OR L_key_value_2 IS NULL)
       AND h.module        = L_module;
  --    
  CURSOR C_freight_matrix IS
    SELECT carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_freight_matrix fm,
           ma_stg_order_drops_detail od,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = od.ship_port
       AND delivery_method       = od.ship_method
       AND freight_forwarder     = od.freight_forward
       AND receiving_point       = od.first_dest       
       AND od.master_order_no   = I_master_order_no
       AND od.order_no          = L_order_no
       AND od.po_type            = L_po_type
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;
  --
  CURSOR C_transportation_matrix IS
    SELECT carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_transportation_matrix tm,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = L_first_dest
       AND delivery_method       = L_ship_method
       AND receiving_point       = L_final_dest
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;
  --
  CURSOR C_hndlcost_matrix IS
    SELECT rate,
           currency  
      FROM ma_hndlcost_matrix hm
     WHERE warehouse = L_first_dest;
  --
  CURSOR C_detail_cost IS
    SELECT *
      FROM (SELECT '1' order_level,
                   o.master_order_no,
                   op.option_id,
                   NULL supplier_reference,
                   s.supplier,
                   NULL manu_country_id,
                   NULL order_no,
                   op.qty_ordered units,
                   NULL first_dest,
                   NULL final_dest,
                   NULL ship_method,
                   NULL supplier_currency,
                   NULL unit_cost,
                   SUM(od.qty_ordered * od.unit_cost) total_unit_cost,
                   NULL total_discount_cost,
                   NULL fc_currency,
                   NULL po_type,
                   NULL retail_price,
                   NULL buy_value,
                   NULL final_dest_date,
                   NULL dept,
                   NULL freight_forward,
                   CASE
                     WHEN o.status = 'A' THEN
                       submitted_date
                     ELSE
                       get_vdate
                   END effective_date
              FROM ma_stg_order o,
                   ma_stg_order_option op,
                   ma_stg_order_drops_detail od,
                   ma_v_sups s,
                   ma_v_wh wh
             WHERE o.master_order_no  = I_master_order_no
               AND op.master_order_no = o.master_order_no
               AND od.master_order_no = op.master_order_no
               AND od.option_id        = op.option_id
               AND s.supplier          = o.supplier
               AND wh.wh               = od.final_dest
             GROUP BY o.master_order_no,
                      op.option_id,
                      s.supplier,
                      op.supplier_reference,
                      op.qty_ordered,
                      CASE
                        WHEN o.status = 'A' THEN
                          submitted_date
                        ELSE
                          get_vdate
                      END
            UNION ALL
            SELECT '2' order_level,
                   o.master_order_no,
                   op.option_id,
                   op.supplier_reference,
                   s.supplier,
                   op.manu_country_id,
                   od.order_no,
                   od.qty_ordered units,
                   od.first_dest,                   
                   od.final_dest,
                   od.ship_method,
                   s.currency_code supplier_currency,
                   od.unit_cost unit_cost,
                   SUM(od.qty_ordered * od.unit_cost) total_unit_cost,
                   NULL total_discount_cost,
                   wh.currency_code fc_currency,
                   od.po_type,
                   unit_retail retail_price,
                   (unit_retail * od.qty_ordered) buy_value,
                   final_dest_date,
                   im.dept,
                   od.freight_forward,
                   CASE
                     WHEN o.status = 'A' THEN
                       submitted_date
                     ELSE
                       get_vdate
                   END effective_date 
              FROM ma_stg_order o,
                   ma_stg_order_option op,
                   ma_stg_order_drops_detail od,
                   ma_v_sups s,
                   ma_v_wh wh,
                   ma_v_item_loc il,
                   ma_v_item_master im
             WHERE o.master_order_no  = I_master_order_no
               AND op.master_order_no = o.master_order_no
               AND od.master_order_no = op.master_order_no
               AND od.option_id        = op.option_id
               AND s.supplier          = o.supplier
               AND wh.wh               = od.final_dest
               AND il.item             = op.option_id
               AND il.loc              = od.final_dest
               AND il.loc_type         = 'W'
               AND im.item             = op.option_id
            GROUP BY o.master_order_no,
                     op.option_id,
                     op.supplier_reference,
                     s.supplier,
                     op.manu_country_id,
                     od.order_no,
                     od.qty_ordered,
                     od.first_dest,
                     od.final_dest,
                     od.ship_method,
                     s.currency_code,
                     od.unit_cost,
                     wh.currency_code,
                     od.po_type,
                     unit_retail,
                     (unit_retail * od.qty_ordered),
                     final_dest_date,
                     im.dept,
                     od.freight_forward,
                     CASE
                        WHEN o.status = 'A' THEN
                          submitted_date
                        ELSE
                          get_vdate
                      END)
    ORDER BY order_level;
  --
  rec_detail_cost C_detail_cost%ROWTYPE;
  --
BEGIN
  --
  --
  --
  DELETE ma_stg_cost_option_detail
    WHERE master_order_no = I_master_order_no;
  --
  OPEN C_detail_cost;
  LOOP
    FETCH C_detail_cost INTO rec_detail_cost;
    EXIT WHEN C_detail_cost%NOTFOUND;    
    --
    L_order_no := rec_detail_cost.order_no;
    L_po_type  := rec_detail_cost.po_type;
    --
    L_total_expense := 0;
    --
    IF rec_detail_cost.order_level <> '1' THEN
      --
      L_supplier           := rec_detail_cost.supplier;
      L_option_id          := rec_detail_cost.option_id;
      L_manu_country_id    := rec_detail_cost.manu_country_id;
      L_first_dest         := rec_detail_cost.first_dest;
      L_final_dest         := rec_detail_cost.final_dest;
      L_ship_method        := rec_detail_cost.ship_method;
      L_freight_forward    := rec_detail_cost.freight_forward;
      L_final_dest_date    := rec_detail_cost.final_dest_date;
      --
      L_carton_fill_rate   := NULL;
      L_per_count          := NULL;
      L_freight_currency   := NULL;
      --
      OPEN C_freight_matrix;
      FETCH C_freight_matrix INTO L_carton_fill_rate,
                                  L_per_count,
                                  L_freight_currency;
      CLOSE C_freight_matrix;
      --dbms_output.put_line('L_carton_fill_rate='||L_carton_fill_rate||' L_per_count='||L_per_count||' L_freight_currency='||L_freight_currency);      
      --
      -- Total Expense Calculation
      --
      --
      -- 1. Freight Cost if Supplier shipping terms are in (FOB, EXW).
      --
      L_freight_cost      := 0;
      L_freight_cost_plan := 0;
      --
      IF L_terms IN ('FOB','EXW') THEN         
        --
        L_freight_cost      := NVL((L_carton_fill_rate/L_per_count),0);
        L_freight_cost_plan := L_freight_cost;
        --
        IF L_freight_currency <> rec_detail_cost.fc_currency THEN
          --
          L_freight_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                       I_currency_out    => L_freight_currency,
                                                       I_currency_in     => rec_detail_cost.fc_currency,
                                                       I_currency_value  => L_freight_cost);
          --
          -- Convert to plan buy margin calculation
          --
          IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                  I_currency_value  => L_freight_cost_plan,
                                  I_currency        => L_freight_currency,
                                  I_currency_out    => rec_detail_cost.fc_currency,
                                  O_currency_value  => L_freight_cost_plan,
                                  I_cost_retail_ind => 'N',
                                  I_effective_date  => rec_detail_cost.effective_date,
                                  I_exchange_type   => 'C') = FALSE THEN
            --
            RAISE FUNCTION_ERROR;
            --
          END IF;
          --
        END IF;
        --
        L_freight_cost      := L_freight_cost * rec_detail_cost.units;
        L_freight_cost_plan := L_freight_cost_plan * rec_detail_cost.units;
        --
      END IF;
      --
      -- 2. Re-processing Cost
      --
      L_re_processing_cost      := 0;   
      L_re_processing_cost_plan := 0;   
      --
      OPEN C_sup_imp_attr;
      FETCH C_sup_imp_attr INTO L_partner_1,
                                L_partner_2;
      CLOSE C_sup_imp_attr;
      --
      -- The re-processing cost will only be calculated if the freight forwarder on PO = partner _1 on sup_import_attr
      --
      IF L_partner_1 = L_freight_forward THEN
        --
        L_cost_component := 0;
        L_rate           := 0;
        L_rate_plan      := 0;
        L_cost_currency  := NULL;
        --
        L_key_value_2    := NULL;
        L_key_value_1    := L_supplier;
        L_module         := 'SUPP';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --
        L_rate_plan := L_rate;
        --
        -- Convert rate into fc currency
        --
        IF L_cost_currency <> rec_detail_cost.fc_currency THEN
          --
          L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                               I_currency_out    => L_cost_currency,
                                               I_currency_in     => rec_detail_cost.fc_currency,
                                               I_currency_value  => L_rate);
          --
          -- Convert to plan buy margin calculation
          --
          IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                  I_currency_value  => L_rate_plan,
                                  I_currency        => L_cost_currency,
                                  I_currency_out    => rec_detail_cost.fc_currency,
                                  O_currency_value  => L_rate_plan,
                                  I_cost_retail_ind => 'N',
                                  I_effective_date  => rec_detail_cost.effective_date,
                                  I_exchange_type   => 'C') = FALSE THEN
            --
            RAISE FUNCTION_ERROR;
            --
          END IF;
          --
        END IF;
        --        
        L_re_processing_cost      := L_rate * rec_detail_cost.units;
        L_re_processing_cost_plan := L_rate_plan * rec_detail_cost.units;
        --        
      END IF;
      --
      -- The labelon cost will only be calculated if the partner_2 field in sup_import_attr table is not null for the supplier site on PO.
      --
      L_labelon_cost      := 0;
      L_labelon_cost_plan := 0;
      --
      IF L_partner_2 IS NOT NULL THEN
        --        
        L_cost_component := 0;
        L_rate           := 0;
        L_cost_currency  := NULL;
        --
        L_key_value_1    := NULL;
        L_key_value_2    := L_partner_2;
        L_module         := 'PTNR';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --
        L_rate_plan := L_rate;
        --
        -- Convert rate into fc currency
        --
        IF L_cost_currency <> rec_detail_cost.fc_currency THEN
          --
          L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                               I_currency_out    => L_cost_currency,
                                               I_currency_in     => rec_detail_cost.fc_currency,
                                               I_currency_value  => L_rate);
          --
          -- Convert to plan buy margin calculation
          --
          IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                  I_currency_value  => L_rate_plan,
                                  I_currency        => L_cost_currency,
                                  I_currency_out    => rec_detail_cost.fc_currency,
                                  O_currency_value  => L_rate_plan,
                                  I_cost_retail_ind => 'N',
                                  I_effective_date  => rec_detail_cost.effective_date,
                                  I_exchange_type   => 'C') = FALSE THEN
            --
            RAISE FUNCTION_ERROR;
            --
          END IF;
          --
        END IF;
        --        
        L_labelon_cost      := L_rate * rec_detail_cost.units;
        L_labelon_cost_plan := L_rate_plan * rec_detail_cost.units;
        --        
      END IF;
      --
      L_total_expense      := L_freight_cost + L_re_processing_cost + L_labelon_cost;
      L_total_expense_plan := L_freight_cost_plan + L_re_processing_cost_plan + L_labelon_cost_plan;
      --
      -- Total Up-Charge Calculation
      --
      --
      -- 1.  Transportation cost and 2. Handling Cost
      --
      L_transportation_cost      := 0;
      L_hndlcost_cost            := 0;
      L_transportation_cost_plan := 0;
      L_hndlcost_cost_plan       := 0;
      --
      IF rec_detail_cost.first_dest <> rec_detail_cost.final_dest THEN
        --
        L_carton_fill_rate        := 0;
        L_per_count               := NULL;
        L_transportation_currency := NULL;
        --
        OPEN C_transportation_matrix;
        FETCH C_transportation_matrix INTO L_carton_fill_rate,
                                           L_per_count,
                                           L_transportation_currency;
        CLOSE C_transportation_matrix;
        --
        L_transportation_cost      := (L_carton_fill_rate/L_per_count);
        L_transportation_cost_plan := L_transportation_cost;
        --
        IF L_transportation_currency <> rec_detail_cost.fc_currency THEN
          --
          L_transportation_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                              I_currency_out    => L_transportation_currency,
                                                              I_currency_in     => rec_detail_cost.fc_currency,
                                                              I_currency_value  => L_transportation_cost);
          --
          -- Convert to plan buy margin calculation
          --
          IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                  I_currency_value  => L_transportation_cost_plan,
                                  I_currency        => L_transportation_currency,
                                  I_currency_out    => rec_detail_cost.fc_currency,
                                  O_currency_value  => L_transportation_cost_plan,
                                  I_cost_retail_ind => 'N',
                                  I_effective_date  => rec_detail_cost.effective_date,
                                  I_exchange_type   => 'C') = FALSE THEN
            --
            RAISE FUNCTION_ERROR;
            --
          END IF;
          --
        END IF;
        --
        L_transportation_cost      := L_transportation_cost * rec_detail_cost.units;
        L_transportation_cost_plan := L_transportation_cost_plan * rec_detail_cost.units;
        --
        -- 2. Handling Cost
        --              
        L_hndlcost_rate     := 0;
        L_hndlcost_currency := NULL;
        --
        OPEN C_hndlcost_matrix;
        FETCH C_hndlcost_matrix INTO L_hndlcost_rate,
                                     L_hndlcost_currency;
        CLOSE C_hndlcost_matrix;
        --
        L_hndlcost_rate_plan := L_hndlcost_rate;
        --
        IF L_hndlcost_currency <> rec_detail_cost.fc_currency THEN
          --
          L_hndlcost_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                        I_currency_out    => L_hndlcost_currency,
                                                        I_currency_in     => rec_detail_cost.fc_currency,
                                                        I_currency_value  => L_hndlcost_rate);
          --
          -- Convert to plan buy margin calculation
          --
          IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                  I_currency_value  => L_hndlcost_rate_plan,
                                  I_currency        => L_hndlcost_currency,
                                  I_currency_out    => rec_detail_cost.fc_currency,
                                  O_currency_value  => L_hndlcost_rate_plan,
                                  I_cost_retail_ind => 'N',
                                  I_effective_date  => rec_detail_cost.effective_date,
                                  I_exchange_type   => 'C') = FALSE THEN
            --
            RAISE FUNCTION_ERROR;
            --
          END IF;
          --
        END IF;
        --
        L_hndlcost_cost      := L_hndlcost_rate * rec_detail_cost.units;
        L_hndlcost_cost_plan := L_hndlcost_rate_plan * rec_detail_cost.units;
        --
      END IF;
      --
      -- Add Transportation to Handling Cost
      --
      L_total_upcharge      := L_transportation_cost + L_hndlcost_cost;
      L_total_upcharge_plan := L_transportation_cost_plan + L_hndlcost_cost_plan;
      --
      -- Duty Cost calculation (Duty Cost will be calculated on a PO only if Supplier shipping terms are in (FOB, EXW))
      --
      L_total_duty      := 0;
      L_total_duty_plan := 0;
      --
      IF L_terms IN ('FOB','EXW') THEN 
        --
        -- Get Commodity code
        --
        L_import_country := NULL;
        L_commodity      := NULL;
        L_av_rate        := 0;
        --
        OPEN C_import_country;
        FETCH C_import_country INTO L_import_country;
        CLOSE C_import_country;
        --
        OPEN C_commodity;
        FETCH C_commodity INTO L_commodity;
        CLOSE C_commodity;            
        --
        -- Check hts_tariff_treatment
        --      
        OPEN C_hts_tariff_treatment;
        FETCH C_hts_tariff_treatment INTO L_av_rate;
        CLOSE C_hts_tariff_treatment;
        --
        L_total_duty := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                   I_currency_out    => rec_detail_cost.supplier_currency,
                                                   I_currency_in     => rec_detail_cost.fc_currency,
                                                   I_currency_value  => rec_detail_cost.total_unit_cost);
        --
        L_total_duty := (L_total_duty + L_total_expense /*+ L_total_upcharge*/) * L_av_rate;
        --
        -- Convert to plan buy margin calculation
        --
        IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                                I_currency_value  => rec_detail_cost.total_unit_cost,
                                I_currency        => rec_detail_cost.supplier_currency,
                                I_currency_out    => rec_detail_cost.fc_currency,
                                O_currency_value  => L_total_duty_plan,
                                I_cost_retail_ind => 'N',
                                I_effective_date  => rec_detail_cost.effective_date,
                                I_exchange_type   => 'C') = FALSE THEN
          --
          RAISE FUNCTION_ERROR;
          --
        END IF;
        --
        L_total_duty_plan := (L_total_duty_plan + L_total_expense_plan) * L_av_rate;
        --
      END IF;
      --            
      -- Total Landed Cost Calculation
      --
      L_total_landed_cost := 0;
      --
      L_total_landed_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                        I_currency_out    => rec_detail_cost.supplier_currency,
                                                        I_currency_in     => rec_detail_cost.fc_currency,
                                                        I_currency_value  => rec_detail_cost.total_unit_cost);
      --
      L_total_landed_cost := NVL(L_total_landed_cost,0) + NVL(L_total_expense,0) + NVL(L_total_upcharge,0) + NVL(L_total_duty,0);
      --
      -- Convert to plan buy margin calculation
      --
      IF CURRENCY_SQL.CONVERT(O_error_message   => L_error_message,
                              I_currency_value  => rec_detail_cost.total_unit_cost,
                              I_currency        => rec_detail_cost.supplier_currency,
                              I_currency_out    => rec_detail_cost.fc_currency,
                              O_currency_value  => L_total_landed_cost_plan,
                              I_cost_retail_ind => 'N',
                              I_effective_date  => rec_detail_cost.effective_date,
                              I_exchange_type   => 'C') = FALSE THEN
        --
        RAISE FUNCTION_ERROR;
        --
      END IF;
      --
      L_total_landed_cost_plan := NVL(L_total_landed_cost_plan,0) + NVL(L_total_expense_plan,0) + NVL(L_total_upcharge_plan,0) + NVL(L_total_duty_plan,0);
      --
      --Get VAT RATE
      --
      -- Reset values
      --
      L_vat_region := NULL;
      L_vat_code   := NULL;
      --
      IF VAT_SQL.GET_VAT_RATE(O_error_message => L_error_message,
                              IO_vat_region   => L_vat_region,
                              IO_vat_code     => L_vat_code,
                              O_vat_rate      => L_vat_rate,
                              I_item          => L_option_id,
                              I_dept          => rec_detail_cost.dept,
                              I_loc_type      => 'W',
                              I_location      => L_final_dest,
                              I_active_date   => rec_detail_cost.final_dest_date,
                              I_vat_type      => 'R') = FALSE THEN
        --
        RAISE FUNCTION_ERROR;
        --
      END IF;
      --
      L_retail_price := rec_detail_cost.retail_price;
      --
      -- Plan Buy Margin Calculation
      --
      /*
      IF CURRENCY_SQL.GET_RATE (O_error_message  => L_error_message,
                                O_exchange_rate  => L_exchange_rate,
                                I_currency_code  => rec_detail_cost.fc_currency,
                                I_exchange_type  => 'C',
                                I_effective_date => rec_detail_cost.effective_date) = FALSE THEN
        --
        RAISE FUNCTION_ERROR;
        --
      END IF;
      --
      L_buy_value    := (rec_detail_cost.buy_value * L_exchange_rate) / (L_vat_rate / 100 + 1);
      L_plan_buy_margin := ((L_buy_value - L_total_landed_cost) / L_buy_value)*100;
      */
      --
      L_buy_value    := rec_detail_cost.buy_value / (L_vat_rate / 100 + 1);
      L_plan_buy_margin := ((L_buy_value - L_total_landed_cost_plan) / L_buy_value)*100;
      --
      -- Exp. Buy Margin Calculation
      --
      /*
      IF CURRENCY_SQL.GET_RATE (O_error_message  => L_error_message,
                                O_exchange_rate  => L_exchange_rate,
                                I_currency_code  => rec_detail_cost.fc_currency,
                                I_exchange_type  => 'O',
                                I_effective_date => rec_detail_cost.effective_date) = FALSE THEN
        --
        RAISE FUNCTION_ERROR;
        --
      END IF;
      --
      L_buy_value    := (rec_detail_cost.buy_value * L_exchange_rate) / (L_vat_rate / 100 + 1);
      */
      L_buy_value    := rec_detail_cost.buy_value / (L_vat_rate / 100 + 1);
      L_exp_buy_margin  := ((L_buy_value - L_total_landed_cost) / L_buy_value)*100;
      --
    END IF;    
    --
    L_supplier  := rec_detail_cost.supplier;
    L_option_id := rec_detail_cost.option_id;
    L_terms     := NULL;
    --    
    OPEN C_terms;
    FETCH C_terms INTO L_terms;
    CLOSE C_terms;
    --
    INSERT INTO ma_stg_cost_option_detail
                 (order_level, 
                  master_order_no, 
                  option_id, 
                  supplier_reference, 
                  order_no, 
                  qty_ordered, 
                  first_dest, 
                  final_dest, 
                  supplier_currency, 
                  unit_cost, 
                  total_unit_cost, 
                  total_discount_cost, 
                  fc_currency, 
                  total_expense, 
                  total_upcharge, 
                  total_duty, 
                  total_landed_cost, 
                  retail_price, 
                  buy_value, 
                  plan_buy_margin, 
                  exp_buy_margin)
           VALUES(rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  rec_detail_cost.option_id, 
                  rec_detail_cost.supplier_reference, 
                  rec_detail_cost.order_no, 
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.final_dest,
                  rec_detail_cost.supplier_currency, 
                  rec_detail_cost.unit_cost, 
                  rec_detail_cost.total_unit_cost, 
                  rec_detail_cost.total_discount_cost, 
                  rec_detail_cost.fc_currency, 
                  DECODE(L_total_expense, 0, NULL, L_total_expense), 
                  DECODE(L_total_upcharge, 0, NULL, L_total_upcharge), 
                  DECODE(L_total_duty, 0, NULL, L_total_duty),  
                  DECODE(L_total_landed_cost, 0, NULL, L_total_landed_cost), 
                  rec_detail_cost.retail_price, 
                  rec_detail_cost.buy_value, 
                  DECODE(L_plan_buy_margin, 0, NULL, L_plan_buy_margin), 
                  DECODE(L_exp_buy_margin, 0, NULL, L_exp_buy_margin));
    --
  END LOOP;
  --
  CLOSE C_detail_cost;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN FUNCTION_ERROR THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => L_error_message);
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_OPTION_DETAILS_COST;
--------------------------------------------------------------------------------
FUNCTION CALC_UP_CHARGE_DETAILS_COST(O_error_message    OUT VARCHAR2,
                                     I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                 VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_OPTION_DETAILS_COST';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
  L_error_message           VARCHAR2(255);
  L_total_expense           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_order_no                MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_po_type                 MA_STG_ORDER_DROPS.PO_TYPE%TYPE;
  L_carton_fill_rate        MA_FREIGHT_MATRIX.CARTON_FILL_RATE%TYPE;
  L_per_count               ELC_COMP.PER_COUNT%TYPE;
  L_freight_currency        MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_transportation_currency MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_transportation_cost     MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_hndlcost_cost           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_supplier                MA_V_SUPS.SUPPLIER%TYPE;
  L_option_id               MA_STG_ORDER_OPTION.OPTION_ID%TYPE;
  L_manu_country_id         MA_STG_ORDER_OPTION.MANU_COUNTRY_ID%TYPE;
  L_first_dest              MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_final_dest              MA_STG_ORDER_DROPS_DETAIL.FINAL_DEST%TYPE;
  L_ship_method             MA_STG_ORDER_DROPS_DETAIL.SHIP_METHOD%TYPE;
  L_hndlcost_rate           MA_HNDLCOST_MATRIX.RATE%TYPE;
  L_hndlcost_currency       MA_HNDLCOST_MATRIX.CURRENCY%TYPE;
  L_cost_component          MA_TRANSPORTATION_MATRIX.COST_COMPONENT%TYPE;
  --  
  CURSOR C_transportation_matrix IS
    SELECT cost_component,
           carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_transportation_matrix tm,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = L_first_dest
       AND delivery_method       = L_ship_method
       AND receiving_point       = L_final_dest
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;
  --
  CURSOR C_hndlcost_matrix IS
    SELECT cost_component,
           rate,
           currency  
      FROM ma_hndlcost_matrix hm
     WHERE warehouse = L_first_dest;
  --
  CURSOR C_detail_cost IS
    WITH tab_skus AS
      (SELECT od.master_order_no, 
              od.option_id, 
              od.order_no, 
              ms.qty_ordered units, 
              od.first_dest first_dest, 
              od.final_dest final_dest, 
              supp_diff_2 supplier_size, 
              ms.sku,
              im.item_desc sku_desc,
              od.po_type,
              mis.supplier,
              op.manu_country_id,
              od.ship_method,
              wh.currency_code fc_currency
          FROM ma_stg_order o,
               ma_stg_order_option op,
               ma_stg_order_drops_detail od,
               ma_stg_sizing_sku ms,
               ma_v_item_master im,
               item_supplier mis,
               wh wh
         WHERE op.master_order_no = o.master_order_no
           AND op.option_id        = od.option_id
           AND o.master_order_no  = od.master_order_no
           AND o.master_order_no  = ms.master_order_no
           AND od.order_no        = ms.order_no
           AND od.first_dest <> od.final_dest
           AND ms.final_dest       = od.final_dest
           AND mis.item            = im.item
           AND mis.supplier        = o.supplier
           AND im.item             = ms.sku
           AND ms.qty_ordered IS NOT NULL
           AND wh.wh               = od.final_dest
           AND o.master_order_no  = I_master_order_no)
    SELECT '1' order_level,
           master_order_no, 
           option_id, 
           order_no,
           sum(units) units,
           first_dest, 
           final_dest,
           NULL supplier_size, 
           NULL sku,
           NULL sku_desc,
           NULL po_type,
           NULL supplier,
           NULL manu_country_id,
           NULL ship_method,
           NULL fc_currency
      FROM tab_skus t
     GROUP BY master_order_no, 
              option_id, 
              order_no,
              first_dest, 
              final_dest
    UNION ALL
    SELECT '2' order_level,
           master_order_no, 
           option_id, 
           order_no,
           units,
           first_dest, 
           final_dest,
           supplier_size, 
           sku,
           sku_desc,
           po_type,
           supplier,
           manu_country_id,
           ship_method,
           fc_currency
      FROM tab_skus t
    ORDER BY order_level,
             master_order_no,
             order_no;
  --
  rec_detail_cost C_detail_cost%ROWTYPE;
  --
BEGIN
  --
  --
  --
  DELETE ma_stg_cost_up_charge_detail
    WHERE master_order_no = I_master_order_no;
  --
  OPEN C_detail_cost;
  LOOP
    FETCH C_detail_cost INTO rec_detail_cost;
    EXIT WHEN C_detail_cost%NOTFOUND;    
    --
    L_order_no := rec_detail_cost.order_no;
    L_po_type   := rec_detail_cost.po_type;
    --
    L_total_expense := NULL;
    --
    IF rec_detail_cost.order_level <> '1' THEN
      --
      L_supplier           := rec_detail_cost.supplier;
      L_option_id          := rec_detail_cost.option_id;
      L_manu_country_id    := rec_detail_cost.manu_country_id;
      L_first_dest         := rec_detail_cost.first_dest;
      L_final_dest         := rec_detail_cost.final_dest;
      L_ship_method        := rec_detail_cost.ship_method;
      --      
      -- 1.  Transportation cost
      --
      L_cost_component          := NULL;
      L_carton_fill_rate        := 0;
      L_per_count               := NULL;      
      L_transportation_currency := NULL;
      --
      OPEN C_transportation_matrix;
      FETCH C_transportation_matrix INTO L_cost_component,
                                         L_carton_fill_rate,
                                         L_per_count,
                                         L_transportation_currency;
      CLOSE C_transportation_matrix;
      --dbms_output.put_line('L_carton_fill_rate='||L_carton_fill_rate||' L_per_count='||L_per_count||' L_freight_currency='||L_freight_currency);  
      --
      --
      IF L_transportation_currency <> rec_detail_cost.fc_currency THEN
        --
        L_carton_fill_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                         I_currency_out    => L_transportation_currency,
                                                         I_currency_in     => rec_detail_cost.fc_currency,
                                                         I_currency_value  => L_carton_fill_rate);
        --
      END IF;
      --
      L_carton_fill_rate    := NVL((L_carton_fill_rate/L_per_count),0);
      L_transportation_cost := L_carton_fill_rate * rec_detail_cost.units;
      --
      -- Insert record for transportation cost
      --
      INSERT INTO ma_stg_cost_up_charge_detail
                 (order_level,
                  master_order_no,
                  option_id,
                  order_no,
                  qty_ordered,
                  first_dest,
                  final_dest,
                  supplier_size,
                  sku,
                  sku_desc,
                  cost_component,
                  currency_code,
                  rate,
                  total_value)
           VALUES(rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  rec_detail_cost.option_id, 
                  rec_detail_cost.order_no, 
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.final_dest,                  
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  rec_detail_cost.sku_desc,
                  L_cost_component,
                  L_transportation_currency,
                  L_carton_fill_rate, 
                  L_transportation_cost);
      --
      -- 2. Handling Cost
      --     
      L_hndlcost_cost     := 0;
      L_cost_component    := NULL;
      L_hndlcost_rate     := 0;
      L_hndlcost_currency := NULL;      
      -- 
      OPEN C_hndlcost_matrix;
      FETCH C_hndlcost_matrix INTO L_cost_component,
                                   L_hndlcost_rate,
                                   L_hndlcost_currency;
      CLOSE C_hndlcost_matrix;
      --
      IF L_hndlcost_currency <> rec_detail_cost.fc_currency THEN
        --
        L_hndlcost_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                      I_currency_out    => L_hndlcost_currency,
                                                      I_currency_in     => rec_detail_cost.fc_currency,
                                                      I_currency_value  => L_hndlcost_rate);
        --
      END IF;
      --
      L_hndlcost_cost := L_hndlcost_rate * rec_detail_cost.units;
      --
      -- Insert record for Handling Cost
      --
      INSERT INTO ma_stg_cost_up_charge_detail
                 (order_level,
                  master_order_no,
                  option_id,
                  order_no,
                  qty_ordered,
                  first_dest,
                  final_dest,
                  supplier_size,
                  sku,
                  sku_desc,
                  cost_component,
                  currency_code,
                  rate,
                  total_value)
           VALUES(rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  rec_detail_cost.option_id, 
                  rec_detail_cost.order_no, 
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.final_dest,                  
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  rec_detail_cost.sku_desc,
                  L_cost_component,
                  L_hndlcost_currency,
                  L_hndlcost_rate, 
                  L_hndlcost_cost);            
      --
    ELSE
      --
      -- insert record for header
      --
      INSERT INTO ma_stg_cost_up_charge_detail
                   (order_level,
                    master_order_no,
                    option_id,
                    order_no,
                    qty_ordered,
                    first_dest,
                    final_dest,
                    supplier_size,
                    sku,
                    sku_desc,
                    cost_component,
                    currency_code,
                    rate,
                    total_value)
             VALUES(rec_detail_cost.order_level,
                    rec_detail_cost.master_order_no, 
                    rec_detail_cost.option_id, 
                    rec_detail_cost.order_no, 
                    rec_detail_cost.units,
                    rec_detail_cost.first_dest, 
                    rec_detail_cost.final_dest,    
                    NULL,              
                    NULL, 
                    NULL,
                    NULL,
                    NULL,
                    NULL, 
                    NULL);
      --
    END IF;  
    --
  END LOOP;
  --
  CLOSE C_detail_cost;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN FUNCTION_ERROR THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => L_error_message);
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_UP_CHARGE_DETAILS_COST;
--------------------------------------------------------------------------------
FUNCTION CALC_DROP_DETAILS_COST(O_error_message    OUT VARCHAR2,
                                I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                 VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_DROP_DETAILS_COST';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
  L_error_message           VARCHAR2(255);
  L_total_expense           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_expense_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_duty              MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_landed_cost       MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_landed_cost             MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_retail_value      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_exp_buy_margin          MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_order_no                MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_old_order_no            MA_STG_ORDER_DROPS.ORDER_NO%TYPE := NULL;
  L_po_type                 MA_STG_ORDER_DROPS.PO_TYPE%TYPE;
  L_carton_fill_rate        MA_FREIGHT_MATRIX.CARTON_FILL_RATE%TYPE;
  L_per_count               ELC_COMP.PER_COUNT%TYPE;
  L_freight_currency        MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_freight_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_re_processing_cost      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_labelon_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_supplier                MA_V_SUPS.SUPPLIER%TYPE;
  L_option_id               MA_STG_ORDER_OPTION.OPTION_ID%TYPE;
  L_manu_country_id         MA_STG_ORDER_OPTION.MANU_COUNTRY_ID%TYPE;
  L_first_dest              MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_old_first_dest          MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_ship_method             MA_STG_ORDER_DROPS_DETAIL.SHIP_METHOD%TYPE;
  L_fc_currency             WH.CURRENCY_CODE%TYPE;
  L_old_fc_currency         WH.CURRENCY_CODE%TYPE;
  L_supplier_currency       WH.CURRENCY_CODE%TYPE;
  L_total_unit_cost         MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_total_discount_cost     MA_STG_ORDER_OPTION.UNIT_COST%TYPE; 
  L_vat_region              VAT_REGION.VAT_REGION%TYPE;
  L_vat_code                VAT_CODE_RATES.VAT_CODE%TYPE;
  L_vat_rate                VAT_CODE_RATES.VAT_RATE%TYPE;
  L_retail_price            MA_V_ITEM_LOC.UNIT_RETAIL%TYPE;
  L_retail_price_vat        MA_V_ITEM_LOC.UNIT_RETAIL%TYPE;
  L_terms                   MA_V_OPTION_SUPPLIER.FREIGHT_TERMS%TYPE;
  L_key_value_1             EXP_PROF_HEAD.KEY_VALUE_1%TYPE;
  L_key_value_2             EXP_PROF_HEAD.KEY_VALUE_2%TYPE;
  L_module                  EXP_PROF_HEAD.MODULE%TYPE;
  L_partner_1               SUP_IMPORT_ATTR.PARTNER_1%TYPE;
  L_partner_2               SUP_IMPORT_ATTR.PARTNER_2%TYPE;
  L_freight_forward         MA_STG_ORDER_DROPS.FREIGHT_FORWARD%TYPE;
  L_import_country          ADDR.COUNTRY_ID%TYPE;
  L_first_dest_date         MA_STG_ORDER_DROPS.FIRST_DEST_DATE%TYPE;
  L_commodity               ITEM_HTS.HTS%TYPE;
  L_rate                    EXP_PROF_DETAIL.COMP_RATE%TYPE;
  L_av_rate                 HTS_TARIFF_TREATMENT.AV_RATE%TYPE;
  L_final_dest_date         MA_STG_ORDER_DROPS_DETAIL.FINAL_DEST_DATE%TYPE;
  L_cost_component          MA_TRANSPORTATION_MATRIX.COST_COMPONENT%TYPE;
  L_cost_currency           EXP_PROF_DETAIL.COMP_CURRENCY%TYPE;
  L_final_dest              MA_STG_ORDER_DROPS_DETAIL.FINAL_DEST%TYPE;
  --
   CURSOR C_import_country IS
    SELECT country_id 
      FROM addr 
     WHERE module      = 'SUPP' 
       AND key_value_1 = L_supplier
       AND addr_type   = '01';
  --  
  CURSOR C_commodity IS
    SELECT hts commodity_code
      FROM item_hts
     WHERE item = L_option_id
       AND import_country_id = L_import_country
       AND origin_country_id = L_manu_country_id
       AND L_final_dest_date BETWEEN effect_from AND effect_to;
  --
  CURSOR C_hts_tariff_treatment IS
    SELECT av_rate 
      FROM hts_tariff_treatment
     WHERE hts = L_commodity
       AND import_country_id = L_import_country
       AND L_first_dest_date BETWEEN effect_from AND effect_to
       /*AND tariff_treatment = tariff_treatment*/; 
  --
  CURSOR C_terms IS
    SELECT code_desc 
      FROM (select distinct
             supplier,
             item,
             freight_terms
             from ma_v_option_supplier),
           code_detail 
     WHERE supplier  = L_supplier 
       AND item      = L_option_id
       AND code_type = 'MSHT'
       AND code      = freight_terms;
  --
  CURSOR C_sup_imp_attr IS
    SELECT NVL(s.partner_1,'Z999') partner_1,
           s.partner_2
      FROM sup_import_attr s
     WHERE s.supplier = L_supplier;
  --  
  CURSOR C_exp_prof IS
    SELECT d.comp_id,
           d.comp_rate,
           d.comp_currency  
      FROM exp_prof_head h,
           exp_prof_detail d 
     WHERE h.exp_prof_key  = d.exp_prof_key
       AND (h.key_value_1  = L_key_value_1 OR L_key_value_1 IS NULL)
       AND (h.key_value_2  = L_key_value_2 OR L_key_value_2 IS NULL)
       AND h.module        = L_module;
  --
  CURSOR C_freight_matrix IS
    SELECT carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_freight_matrix fm,
           ma_stg_order_drops_detail od,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = od.ship_port
       AND delivery_method       = od.ship_method
       AND freight_forwarder     = od.freight_forward
       AND receiving_point       = od.first_dest       
       AND od.master_order_no    = I_master_order_no
       AND od.order_no           = L_order_no
       AND od.po_type            = L_po_type
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;  
  --
  /*
  CURSOR C_hndlcost_matrix IS
    SELECT rate,
           currency  
      FROM ma_hndlcost_matrix hm
     WHERE warehouse = L_first_dest;
  */
  --
  CURSOR C_detail_cost IS
    SELECT od.master_order_no, 
           od.order_no, 
           od.option_id,
           od.unit_cost,
           od.qty_ordered units,
           (od.unit_cost * od.qty_ordered) tot_unit_cost,
           od.first_dest, 
           od.po_type,
           o.supplier supplier,
           s.currency_code supplier_currency,
           op.manu_country_id,
           od.ship_method,
           wh.currency_code fc_currency,
           im.dept,
           od.first_dest_date,
           NULL total_discount_cost,
           unit_retail retail_price,
           (unit_retail * od.qty_ordered) buy_value,
           od.freight_forward,
           od.final_dest,
           od.final_dest_date,
           CASE
             WHEN o.status = 'A' THEN
               submitted_date
             ELSE
               get_vdate
           END effective_date 
      FROM ma_stg_order o,
           ma_stg_order_option op,
           ma_stg_order_drops_detail od,
           wh wh,
           ma_v_sups s,
           ma_v_item_loc il,
           ma_v_item_master im
     WHERE op.master_order_no = o.master_order_no
       AND op.option_id       = od.option_id
       AND o.master_order_no  = od.master_order_no
       AND wh.wh              = od.first_dest
       AND s.supplier         = o.supplier
       AND im.item            = od.option_id
       AND il.item            = op.option_id
       AND il.loc             = od.first_dest
       AND il.loc_type        = 'W'
       AND o.master_order_no  = I_master_order_no
    ORDER BY od.order_no;
  --
  rec_detail_cost C_detail_cost%ROWTYPE;
  --
BEGIN
  --
  --
  --
  DELETE ma_stg_cost_drop_detail
    WHERE master_order_no = I_master_order_no;
  --
  OPEN C_detail_cost;
  LOOP
    FETCH C_detail_cost INTO rec_detail_cost;
    EXIT WHEN C_detail_cost%NOTFOUND;    
    --
    L_order_no          := rec_detail_cost.order_no;
    L_po_type           := rec_detail_cost.po_type;
    L_supplier          := rec_detail_cost.supplier;
    L_option_id         := rec_detail_cost.option_id;
    L_manu_country_id   := rec_detail_cost.manu_country_id;
    L_first_dest        := rec_detail_cost.first_dest;
    L_ship_method       := rec_detail_cost.ship_method;
    L_fc_currency       := rec_detail_cost.fc_currency;
    L_supplier_currency := rec_detail_cost.supplier_currency;
    L_final_dest        := rec_detail_cost.final_dest;
    L_freight_forward   := rec_detail_cost.freight_forward;
    L_final_dest_date   := rec_detail_cost.final_dest_date;
    --
    -- Get Terms
    --
    L_terms     := NULL;
    --    
    OPEN C_terms;
    FETCH C_terms INTO L_terms;
    CLOSE C_terms;
    --
    -- Calculate values
    --
    IF NVL(L_old_order_no, -9999) <> L_order_no THEN -- New Drop
      --
      IF L_old_order_no IS NOT NULL THEN
        --
        -- Convert to supplier Currency
        -- 
        /*
        L_total_unit_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                        I_currency_out    => L_supplier_currency,
                                                        I_currency_in     => L_old_fc_currency,
                                                        I_currency_value  => L_total_unit_cost);
        --
        L_total_discount_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                            I_currency_out    => L_supplier_currency,
                                                            I_currency_in     => L_old_fc_currency,
                                                            I_currency_value  => L_total_discount_cost);
        --
        */
        INSERT INTO ma_stg_cost_drop_detail
                     (master_order_no, 
                      order_no, 
                      fc, 
                      currency_code, 
                      total_unit_cost, 
                      total_discount_cost, 
                      total_expense, 
                      total_duty, 
                      total_landed_cost, 
                      total_retail_value, 
                      exp_buy_margin)
               VALUES(I_master_order_no, 
                      L_old_order_no, 
                      L_old_first_dest, 
                      L_old_fc_currency, 
                      L_total_unit_cost, 
                      L_total_discount_cost, 
                      L_total_expense, 
                      L_total_duty, 
                      L_total_landed_cost, 
                      L_total_retail_value, 
                      L_exp_buy_margin);
      END IF;
      --
      L_total_unit_cost     := 0;
      L_total_discount_cost := 0; 
      L_total_expense       := 0;
      L_total_duty          := 0;
      L_total_landed_cost   := 0;
      L_total_retail_value  := 0;
      L_exp_buy_margin      := 0;
      --      
    END IF;
    --
    -- Total Unit Cost Calculation
    --    
    L_total_unit_cost := L_total_unit_cost + rec_detail_cost.tot_unit_cost;
    --
    -- Total Discount Cost Calculation
    --
    --L_total_discount_cost := 0;
    --
    -- Total Expense Calculation
    --
    --
    -- 1. Freight Cost if Supplier shipping terms are in (FOB, EXW).
    --
    L_freight_cost := 0;
    --
    IF L_terms IN ('FOB','EXW') THEN      
      --
      L_carton_fill_rate   := 0;
      L_per_count          := NULL;
      L_freight_currency   := NULL;
      --
      OPEN C_freight_matrix;
      FETCH C_freight_matrix INTO L_carton_fill_rate,
                                  L_per_count,
                                  L_freight_currency;
      CLOSE C_freight_matrix;
      --dbms_output.put_line('L_carton_fill_rate='||L_carton_fill_rate||' L_per_count='||L_per_count||' L_freight_currency='||L_freight_currency);          
      --
      -- 1. Freight Cost
      --
      L_freight_cost := NVL((L_carton_fill_rate/L_per_count),0);
      --
      IF L_freight_currency <> L_fc_currency THEN
        --
        L_freight_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                     I_currency_out    => L_freight_currency,
                                                     I_currency_in     => L_fc_currency,
                                                     I_currency_value  => L_freight_cost);
        --
      END IF;
      --
      L_freight_cost := L_freight_cost * rec_detail_cost.units;
      --
    END IF;
    --
    -- 2. Re-processing Cost
    --
    L_re_processing_cost := 0;
    --
    OPEN C_sup_imp_attr;
    FETCH C_sup_imp_attr INTO L_partner_1,
                              L_partner_2;
    CLOSE C_sup_imp_attr;
    --
    -- The re-processing cost will only be calculated if the freight forwarder on PO = partner _1 on sup_import_attr
    --
    IF L_partner_1 = L_freight_forward THEN
      --
      L_cost_component := NULL;
      L_rate           := 0;
      L_cost_currency  := NULL;
      --
      L_key_value_2    := NULL;
      L_key_value_1    := L_supplier;
      L_module         := 'SUPP';
      -- 
      OPEN C_exp_prof;
      FETCH C_exp_prof INTO L_cost_component,
                            L_rate,
                            L_cost_currency;                              
      CLOSE C_exp_prof;
      --
      -- Convert rate into fc currency
      --
      IF L_cost_currency <> rec_detail_cost.fc_currency THEN
        --
        L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                             I_currency_out    => L_cost_currency,
                                             I_currency_in     => rec_detail_cost.fc_currency,
                                             I_currency_value  => L_rate);
        --
      END IF;
      --        
      L_re_processing_cost := L_rate * rec_detail_cost.units;
      --        
    END IF;
    --
    -- The labelon cost will only be calculated if the partner_2 field in sup_import_attr table is not null for the supplier site on PO.
    --
    L_labelon_cost := 0;
    --
    IF L_partner_2 IS NOT NULL THEN
      --
      L_cost_component := NULL;
      L_rate           := 0;
      L_cost_currency  := NULL;
      --
      L_key_value_1    := NULL;
      L_key_value_2    := L_partner_2;
      L_module         := 'PTNR';
      -- 
      OPEN C_exp_prof;
      FETCH C_exp_prof INTO L_cost_component,
                            L_rate,
                            L_cost_currency;                              
      CLOSE C_exp_prof;
      --
      -- Convert rate into fc currency
      --
      IF L_cost_currency <> rec_detail_cost.fc_currency THEN
        --
        L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                             I_currency_out    => L_cost_currency,
                                             I_currency_in     => rec_detail_cost.fc_currency,
                                             I_currency_value  => L_rate);
        --
      END IF;
      --        
      L_labelon_cost := L_rate * rec_detail_cost.units;
      --        
    END IF;
    --
    L_expense_cost  := (L_freight_cost + L_re_processing_cost + L_labelon_cost); 
    L_total_expense := L_total_expense + L_expense_cost;    
    --    
    --
    -- Duty Cost calculation (Duty Cost will be calculated on a PO only if Supplier shipping terms are in (FOB, EXW))
    --
    IF L_terms IN ('FOB','EXW') THEN 
      --
      -- Get Commodity code
      --
      L_import_country := NULL;
      L_commodity      := NULL;
      L_av_rate        := 0;
      --
      OPEN C_import_country;
      FETCH C_import_country INTO L_import_country;
      CLOSE C_import_country;
      --
      OPEN C_commodity;
      FETCH C_commodity INTO L_commodity;
      CLOSE C_commodity;            
      --
      -- Check hts_tariff_treatment
      --      
      OPEN C_hts_tariff_treatment;
      FETCH C_hts_tariff_treatment INTO L_av_rate;
      CLOSE C_hts_tariff_treatment;
      --
      L_total_duty := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                 I_currency_out    => rec_detail_cost.supplier_currency,
                                                 I_currency_in     => rec_detail_cost.fc_currency,
                                                 I_currency_value  => rec_detail_cost.unit_cost);
      --
      L_total_duty := (L_total_duty + L_expense_cost) * L_av_rate;
      --
    END IF;
    --            
    -- Total Landed Cost Calculation
    --
    L_landed_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                I_currency_out    => rec_detail_cost.supplier_currency,
                                                I_currency_in     => rec_detail_cost.fc_currency,
                                                I_currency_value  => rec_detail_cost.unit_cost);
    --
    L_landed_cost := (NVL(L_landed_cost,0) + NVL(L_expense_cost/rec_detail_cost.units,0) + NVL(L_total_duty,0)) * rec_detail_cost.units;
    L_total_landed_cost := L_total_landed_cost + L_landed_cost;
    /*
    --        
    -- Calculate values without VAT
    --
    IF VAT_SQL.GET_VAT_RATE(O_error_message => L_error_message,
                            IO_vat_region   => L_vat_region,
                            IO_vat_code     => L_vat_code,
                            O_vat_rate      => L_vat_rate,
                            I_item          => L_option_id,
                            I_dept          => rec_detail_cost.dept,
                            I_loc_type      => 'W',
                            I_location      => L_first_dest,
                            I_active_date   => rec_detail_cost.first_dest_date,
                            I_vat_type      => 'R') = FALSE THEN
      --
      RAISE FUNCTION_ERROR;
      --
    END IF;
    --
    L_retail_price := (rec_detail_cost.retail_price / (L_vat_rate / 100 + 1)) * rec_detail_cost.units;
    */
    L_retail_price := rec_detail_cost.retail_price * rec_detail_cost.units;
    L_total_retail_value := L_total_retail_value + L_retail_price;
    --
    --L_buy_value    := rec_detail_cost.buy_value / (L_vat_rate / 100 + 1);
    --
    -- Buy Margin Calculation
    --
    /*IF CURRENCY_SQL.GET_RATE (O_error_message  => L_error_message,
                              O_exchange_rate  => L_vat_rate,
                              I_currency_code  => rec_detail_cost.fc_currency,
                              I_exchange_type  => 'O',
                              I_effective_date => rec_detail_cost.effective_date) = FALSE THEN
      --
      RAISE FUNCTION_ERROR;
      --
    END IF;*/
    --
    L_vat_region := NULL;
    L_vat_code   := NULL;
    --
    IF VAT_SQL.GET_VAT_RATE(O_error_message => L_error_message,
                            IO_vat_region   => L_vat_region,
                            IO_vat_code     => L_vat_code,
                            O_vat_rate      => L_vat_rate,
                            I_item          => L_option_id,
                            I_dept          => rec_detail_cost.dept,
                            I_loc_type      => 'W',
                            I_location      => L_final_dest,
                            I_active_date   => rec_detail_cost.final_dest_date,
                            I_vat_type      => 'R') = FALSE THEN
      --
      RAISE FUNCTION_ERROR;
      --
    END IF;
    --
    L_retail_price_vat := (/*rec_detail_cost.retail_price*/ L_retail_price/ (L_vat_rate / 100 + 1)) /** rec_detail_cost.units*/;
    L_exp_buy_margin  := (L_exp_buy_margin + ((L_retail_price_vat - L_landed_cost) / L_retail_price_vat)) * 100;
    --
    -- Old Values
    --
    L_old_order_no    := L_order_no; 
    L_old_first_dest  := L_first_dest; 
    L_old_fc_currency := L_fc_currency; 
    --    
  END LOOP;
  --
  -- Insert last Record
  --
  IF L_old_order_no IS NOT NULL THEN
    /*
    --
    -- Convert to supplier Currency
    -- 
    L_total_unit_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                    I_currency_out    => L_supplier_currency,
                                                    I_currency_in     => L_old_fc_currency,
                                                    I_currency_value  => L_total_unit_cost);
    --
    L_total_discount_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                        I_currency_out    => L_supplier_currency,
                                                        I_currency_in     => L_old_fc_currency,
                                                        I_currency_value  => L_total_discount_cost);
    --
    */
    INSERT INTO ma_stg_cost_drop_detail
                 (master_order_no, 
                  order_no, 
                  fc, 
                  currency_code, 
                  total_unit_cost, 
                  total_discount_cost, 
                  total_expense, 
                  total_duty, 
                  total_landed_cost, 
                  total_retail_value, 
                  exp_buy_margin)
           VALUES(I_master_order_no, 
                  L_old_order_no, 
                  L_old_first_dest, 
                  L_old_fc_currency, 
                  L_total_unit_cost, 
                  L_total_discount_cost, 
                  L_total_expense, 
                  L_total_duty, 
                  L_total_landed_cost, 
                  L_total_retail_value, 
                  L_exp_buy_margin);
  END IF;
  --
  CLOSE C_detail_cost;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN FUNCTION_ERROR THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => L_error_message);
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_OPTION_DETAILS_COST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_DROP_DETAILS_COST;
--------------------------------------------------------------------------------
FUNCTION CALC_EXPENSE_DETAILS_COST(O_error_message    OUT VARCHAR2,
                                   I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                 VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_EXPENSE_DETAILS_COST';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
  L_error_message           VARCHAR2(255);
  L_total_expense           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_order_no                MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_po_type                 MA_STG_ORDER_DROPS.PO_TYPE%TYPE;
  L_carton_fill_rate        MA_FREIGHT_MATRIX.CARTON_FILL_RATE%TYPE;
  L_per_count               ELC_COMP.PER_COUNT%TYPE;
  L_freight_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_re_processing_cost      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;  
  L_freight_currency        MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_supplier                MA_V_SUPS.SUPPLIER%TYPE;
  L_option_id               MA_STG_ORDER_OPTION.OPTION_ID%TYPE;
  L_manu_country_id         MA_STG_ORDER_OPTION.MANU_COUNTRY_ID%TYPE;
  L_first_dest              MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_ship_method             MA_STG_ORDER_DROPS_DETAIL.SHIP_METHOD%TYPE;
  L_cost_component          MA_TRANSPORTATION_MATRIX.COST_COMPONENT%TYPE;
  L_terms                   MA_V_OPTION_SUPPLIER.FREIGHT_TERMS%TYPE;
  L_freight_forward         MA_STG_ORDER_DROPS.FREIGHT_FORWARD%TYPE;
  L_partner_1               SUP_IMPORT_ATTR.PARTNER_1%TYPE;
  L_partner_2               SUP_IMPORT_ATTR.PARTNER_2%TYPE;
  L_total_value             MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_rate                    EXP_PROF_DETAIL.COMP_RATE%TYPE;
  L_cost_currency           EXP_PROF_DETAIL.COMP_CURRENCY%TYPE;
  L_key_value_1             EXP_PROF_HEAD.KEY_VALUE_1%TYPE;
  L_key_value_2             EXP_PROF_HEAD.KEY_VALUE_2%TYPE;
  L_module                  EXP_PROF_HEAD.MODULE%TYPE;
  L_labelon_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  --  
  CURSOR C_terms IS
    SELECT code_desc 
      FROM (select distinct
             supplier,
             item,
             freight_terms
             from ma_v_option_supplier),
           code_detail 
     WHERE supplier  = L_supplier 
       AND item      = L_option_id
       AND code_type = 'MSHT'
       AND code      = freight_terms;
  --
  CURSOR C_sup_imp_attr IS
    SELECT NVL(s.partner_1,'Z999') partner_1,
           s.partner_2
      FROM sup_import_attr s
     WHERE s.supplier = L_supplier;
  --
  CURSOR C_freight_matrix IS
    SELECT cost_component,
           carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_freight_matrix fm,
           ma_stg_order_drops_detail od,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = od.ship_port
       AND delivery_method       = od.ship_method
       AND freight_forwarder     = od.freight_forward
       AND receiving_point       = od.first_dest       
       AND od.master_order_no    = I_master_order_no
       AND od.order_no           = L_order_no
       AND od.po_type            = L_po_type
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;
  --  
  CURSOR C_exp_prof IS
    SELECT d.comp_id,
           d.comp_rate,
           d.comp_currency  
      FROM exp_prof_head h,
           exp_prof_detail d 
     WHERE h.exp_prof_key  = d.exp_prof_key
       AND (h.key_value_1  = L_key_value_1 OR L_key_value_1 IS NULL)
       AND (h.key_value_2  = L_key_value_2 OR L_key_value_2 IS NULL)
       AND h.module        = L_module;
  --
  CURSOR C_detail_cost IS
    WITH tab_skus AS
      (SELECT op.master_order_no, 
              op.option_id,
              im1.item_desc option_desc, 
              od.order_no, 
              ms.qty_ordered units, 
              od.first_dest first_dest,
              od.final_dest, 
              supp_diff_2 supplier_size, 
              ms.sku,
              im.item_desc sku_desc,
              od.po_type,
              mis.supplier,
              op.manu_country_id,
              od.ship_method,
              wh.currency_code fc_currency,
              wh.wh_name fc_desc,
              od.freight_forward
          FROM ma_stg_order o,
               ma_stg_order_option op,
               ma_stg_order_drops_detail od,
               ma_stg_sizing_sku ms,
               ma_v_item_master im,
               ma_v_item_master im1,
               item_supplier mis,
               wh wh
         WHERE op.master_order_no = o.master_order_no
           AND (op.option_id = od.option_id OR od.option_id IS NULL)
           AND o.master_order_no  = od.master_order_no
           AND o.master_order_no  = ms.master_order_no
           AND od.order_no        = ms.order_no
           AND mis.item           = im.item
           AND im1.item           = op.option_id
           AND mis.supplier       = o.supplier
           AND im.item            = ms.sku
           AND wh.wh              = od.first_dest
           AND od.seq_no          = ms.seq_no
           AND o.master_order_no  = I_master_order_no
           AND ms.qty_ordered IS NOT NULL)
    SELECT order_level,
           master_order_no, 
           option_id,
           option_desc, 
           order_no,
           units,
           first_dest, 
           supplier_size, 
           sku,
           sku_desc,
           po_type,
           supplier,
           manu_country_id,
           ship_method,
           fc_currency,
           fc_desc,
           freight_forward
      FROM (SELECT '1' order_level,
                   master_order_no, 
                   option_id, 
                   option_desc,
                   order_no,
                   sum(units) units,
                   first_dest, 
                   NULL supplier_size, 
                   NULL sku,
                   NULL sku_desc,
                   NULL po_type,
                   supplier,
                   NULL manu_country_id,
                   NULL ship_method,
                   NULL fc_currency,
                   fc_desc,
                   NULL freight_forward
              FROM tab_skus t
             GROUP BY master_order_no, 
                      option_id,
                      supplier, 
                      option_desc,
                      order_no,
                      first_dest,
                      fc_desc
            UNION ALL
            SELECT '2' order_level,
                   master_order_no, 
                   option_id, 
                   option_desc,
                   order_no,
                   SUM(units) units,
                   first_dest, 
                   supplier_size, 
                   sku,
                   sku_desc,
                   'D' po_type,
                   supplier,
                   manu_country_id,
                   ship_method,
                   fc_currency,
                   fc_desc,
                   freight_forward
              FROM tab_skus t
            GROUP BY master_order_no, 
                     option_id, 
                     option_desc,
                     order_no,
                     first_dest, 
                     supplier_size, 
                     sku,
                     sku_desc,
                     --po_type,
                     supplier,
                     manu_country_id,
                     ship_method,
                     fc_currency,
                     fc_desc,
                     freight_forward)
    ORDER BY master_order_no, 
             order_no,
             order_level,
             option_id,
             sku;
  --
  rec_detail_cost C_detail_cost%ROWTYPE;
  --
BEGIN
  --
  --
  --
  DELETE ma_stg_cost_expense_detail
    WHERE master_order_no = I_master_order_no;
  --
  OPEN C_detail_cost;
  LOOP
    FETCH C_detail_cost INTO rec_detail_cost;
    EXIT WHEN C_detail_cost%NOTFOUND;    
    --
    L_order_no := rec_detail_cost.order_no;
    L_po_type   := rec_detail_cost.po_type;
    --
    L_total_expense := NULL;
    --
    IF rec_detail_cost.order_level <> '1' THEN
      --
      L_supplier           := rec_detail_cost.supplier;
      L_option_id          := rec_detail_cost.option_id;
      L_manu_country_id    := rec_detail_cost.manu_country_id;
      L_first_dest         := rec_detail_cost.first_dest;
      L_ship_method        := rec_detail_cost.ship_method;
      L_freight_forward    := rec_detail_cost.freight_forward;                  
      --
      -- Freight cost will only be calculated if the supplier shipping terms are in 'FOB' or 'EXW'.
      --
      L_freight_cost := 0;
      --
      IF L_terms IN ('FOB','EXW') THEN        
        --
        L_carton_fill_rate   := 0;
        L_per_count          := NULL;
        L_freight_currency   := NULL;
        L_cost_component     := NULL;
        --
        OPEN C_freight_matrix;
        FETCH C_freight_matrix INTO L_cost_component,
                                    L_carton_fill_rate,
                                    L_per_count,
                                    L_freight_currency;
        CLOSE C_freight_matrix;
        --        
        L_freight_cost := NVL((L_carton_fill_rate/L_per_count),0);
        --
        IF L_freight_currency <> rec_detail_cost.fc_currency THEN
          --
          L_freight_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                       I_currency_out    => L_freight_currency,
                                                       I_currency_in     => rec_detail_cost.fc_currency,
                                                       I_currency_value  => L_freight_cost);
          --
        END IF;
        --
        L_total_value := L_freight_cost * rec_detail_cost.units;
        --
        -- Insert Freight cost
        -- 
        INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  rec_detail_cost.order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  L_cost_component,
                  L_freight_currency,
                  L_freight_cost,
                  L_total_value);
        --
      END IF;
      --
      OPEN C_sup_imp_attr;
      FETCH C_sup_imp_attr INTO L_partner_1,
                                L_partner_2;
      CLOSE C_sup_imp_attr;
      --
      -- The re-processing cost will only be calculated if the freight forwarder on PO = partner _1 on sup_import_attr
      --
      L_re_processing_cost := 0;
      --
      IF L_partner_1 = L_freight_forward THEN
        --
        L_cost_component := NULL;
        L_rate           := 0;
        L_cost_currency  := NULL;
        --
        L_key_value_2    := NULL;
        L_key_value_1    := L_supplier;
        L_module         := 'SUPP';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --  
        IF L_cost_component IS NOT NULL THEN
          --
          -- Convert rate into fc currency
          --
          IF L_cost_currency <> rec_detail_cost.fc_currency THEN
            --
            L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                 I_currency_out    => L_cost_currency,
                                                 I_currency_in     => rec_detail_cost.fc_currency,
                                                 I_currency_value  => L_rate);
            --
          END IF;
          --      
          L_re_processing_cost := L_rate * rec_detail_cost.units;
          --
          -- Insert re-processing cost
          -- 
          INSERT INTO ma_stg_cost_expense_detail
                   (expense_id,
                    order_level, 
                    master_order_no, 
                    order_no, 
                    option_id, 
                    qty_ordered, 
                    fc, 
                    fc_desc, 
                    supplier_size, 
                    sku, 
                    cost_component, 
                    currency_code, 
                    rate, 
                    total_value)
             VALUES(ma_stg_cost_expense_detail_seq.nextval,
                    rec_detail_cost.order_level,
                    rec_detail_cost.master_order_no, 
                    rec_detail_cost.order_no, 
                    rec_detail_cost.option_id,
                    rec_detail_cost.units,
                    rec_detail_cost.first_dest, 
                    rec_detail_cost.fc_desc,                   
                    rec_detail_cost.supplier_size, 
                    rec_detail_cost.sku,
                    L_cost_component,
                    L_cost_currency,
                    L_rate,
                    L_re_processing_cost);
          --
        END IF;
        --
      END IF;
      --
      -- The labelon cost will only be calculated if the partner_2 field in sup_import_attr table is not null for the supplier site on PO.
      --
      L_labelon_cost := 0;
      --
      IF L_partner_2 IS NOT NULL THEN
        --
        L_cost_component := NULL;
        L_rate           := 0;
        L_cost_currency  := NULL;
        --
        L_key_value_1    := NULL;
        L_key_value_2    := L_partner_2;
        L_module         := 'PTNR';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --        
        IF L_cost_component IS NOT NULL THEN
          --
          -- Convert rate into fc currency
          --
          IF L_cost_currency <> rec_detail_cost.fc_currency THEN
            --
            L_rate := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                 I_currency_out    => L_cost_currency,
                                                 I_currency_in     => rec_detail_cost.fc_currency,
                                                 I_currency_value  => L_rate);
            --
          END IF;
          --
          L_labelon_cost := L_rate * rec_detail_cost.units;
          --
          -- Insert labelon cost
          -- 
          INSERT INTO ma_stg_cost_expense_detail
                   (expense_id,
                    order_level, 
                    master_order_no, 
                    order_no, 
                    option_id, 
                    qty_ordered, 
                    fc, 
                    fc_desc, 
                    supplier_size, 
                    sku, 
                    cost_component, 
                    currency_code, 
                    rate, 
                    total_value)
             VALUES(ma_stg_cost_expense_detail_seq.nextval,
                    rec_detail_cost.order_level,
                    rec_detail_cost.master_order_no, 
                    rec_detail_cost.order_no, 
                    rec_detail_cost.option_id,
                    rec_detail_cost.units,
                    rec_detail_cost.first_dest, 
                    rec_detail_cost.fc_desc,                   
                    rec_detail_cost.supplier_size, 
                    rec_detail_cost.sku,
                    L_cost_component,
                    L_cost_currency,
                    L_rate,
                    L_labelon_cost);
          --
        END IF;
        --
      END IF;
      --
    ELSE
      --
      -- Check terms
      --
      L_supplier  := rec_detail_cost.supplier;
      L_option_id := rec_detail_cost.option_id;
      --
      OPEN C_terms;
      FETCH C_terms INTO L_terms;
      CLOSE C_terms;
      --
      -- insert record for header
      --            
      INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  rec_detail_cost.order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL);
      --
    END IF;  
    --
  END LOOP;
  --
  CLOSE C_detail_cost;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN FUNCTION_ERROR THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_EXPENSE_DETAILS_COST',
                                              I_error_backtrace   => L_error_message);
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_EXPENSE_DETAILS_COST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_EXPENSE_DETAILS_COST;
--------------------------------------------------------------------------------
FUNCTION CALC_DUTY_DETAILS_COST(O_error_message    OUT VARCHAR2,
                                I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                 VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_DUTY_DETAILS_COST';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
  L_error_message           VARCHAR2(255);
  L_total_expense           MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_order_no                MA_STG_ORDER_DROPS.ORDER_NO%TYPE;
  L_po_type                 MA_STG_ORDER_DROPS.PO_TYPE%TYPE;
  L_carton_fill_rate        MA_FREIGHT_MATRIX.CARTON_FILL_RATE%TYPE;
  L_per_count               ELC_COMP.PER_COUNT%TYPE;
  L_freight_cost            MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_re_processing_cost      MA_STG_ORDER_OPTION.UNIT_COST%TYPE;  
  L_freight_currency        MA_FREIGHT_MATRIX.CURRENCY%TYPE;
  L_supplier                MA_V_SUPS.SUPPLIER%TYPE;
  L_option_id               MA_STG_ORDER_OPTION.OPTION_ID%TYPE;
  L_manu_country_id         MA_STG_ORDER_OPTION.MANU_COUNTRY_ID%TYPE;
  L_first_dest              MA_STG_ORDER_DROPS_DETAIL.FIRST_DEST%TYPE;
  L_ship_method             MA_STG_ORDER_DROPS_DETAIL.SHIP_METHOD%TYPE;
  L_cost_component          MA_TRANSPORTATION_MATRIX.COST_COMPONENT%TYPE;
  L_terms                   MA_V_OPTION_SUPPLIER.FREIGHT_TERMS%TYPE;
  L_freight_forward         MA_STG_ORDER_DROPS.FREIGHT_FORWARD%TYPE;
  L_partner_1               SUP_IMPORT_ATTR.PARTNER_1%TYPE;
  L_partner_2               SUP_IMPORT_ATTR.PARTNER_2%TYPE;
  L_total_value             MA_STG_ORDER_OPTION.UNIT_COST%TYPE;
  L_rate                    EXP_PROF_DETAIL.COMP_RATE%TYPE;
  L_cost_currency           EXP_PROF_DETAIL.COMP_CURRENCY%TYPE;
  L_key_value_1             EXP_PROF_HEAD.KEY_VALUE_1%TYPE;
  L_module                  EXP_PROF_HEAD.MODULE%TYPE;
  L_import_country          ADDR.COUNTRY_ID%TYPE;
  L_first_dest_date         MA_STG_ORDER_DROPS.FIRST_DEST_DATE%TYPE;
  L_commodity               ITEM_HTS.HTS%TYPE;
  L_av_rate                 HTS_TARIFF_TREATMENT.AV_RATE%TYPE;
  --
  CURSOR C_import_country IS
    SELECT country_id 
      FROM addr 
     WHERE module      = 'SUPP' 
       AND key_value_1 = L_supplier
       AND addr_type   = '01';
  --  
  CURSOR C_commodity IS
    SELECT hts commodity_code
      FROM item_hts
     WHERE item = L_option_id
       AND import_country_id = L_import_country
       AND origin_country_id = L_manu_country_id
       AND L_first_dest_date BETWEEN effect_from AND effect_to;
  --
  CURSOR C_hts_tariff_treatment IS
    SELECT av_rate 
      FROM hts_tariff_treatment
     WHERE hts = L_commodity
       AND import_country_id = L_import_country
       AND L_first_dest_date BETWEEN effect_from AND effect_to
       /*AND tariff_treatment = tariff_treatment*/; 
  --
  CURSOR C_terms IS
    SELECT code_desc 
      FROM (select distinct
             supplier,
             item,
             freight_terms
             from ma_v_option_supplier),
           code_detail 
     WHERE supplier  = L_supplier 
       AND item      = L_option_id
       AND code_type = 'MSHT'
       AND code      = freight_terms;
  --
  CURSOR C_sup_imp_attr IS
    SELECT NVL(s.partner_1,'Z999') partner_1,
           s.partner_2
      FROM sup_import_attr s
     WHERE s.supplier = L_supplier;
  --
  CURSOR C_freight_matrix IS
    SELECT cost_component,
           carton_fill_rate,
           number_11 per_count,
           currency  
      FROM ma_freight_matrix fm,
           ma_stg_order_drops_detail od,
           item_supp_country_cfa_ext isc
     WHERE shipping_point        = od.ship_port
       AND delivery_method       = od.ship_method
       AND freight_forwarder     = od.freight_forward
       AND receiving_point       = od.first_dest       
       AND od.master_order_no    = I_master_order_no
       AND od.order_no           = L_order_no
       AND od.po_type            = L_po_type
       AND isc.item              = L_option_id
       AND isc.supplier          = L_supplier
       AND isc.origin_country_id = L_manu_country_id;
  --  
  CURSOR C_exp_prof IS
    SELECT d.comp_id,
           d.comp_rate,
           d.comp_currency  
      FROM exp_prof_head h,
           exp_prof_detail d 
     WHERE h.exp_prof_key  = d.exp_prof_key
       AND h.key_value_1   = L_key_value_1
       AND h.module = L_module;
  --
  CURSOR C_detail_cost IS
    WITH tab_skus AS
      (SELECT od.master_order_no, 
              od.option_id,
              im1.item_desc option_desc, 
              od.order_no, 
              ms.qty_ordered units, 
              od.first_dest first_dest,
              od.first_dest_date,
              od.final_dest, 
              supp_diff_2 supplier_size, 
              ms.sku,
              im.item_desc sku_desc,
              od.po_type,
              mis.supplier,
              op.manu_country_id,
              od.ship_method,
              wh.currency_code fc_currency,
              wh.wh_name fc_desc,
              od.freight_forward
          FROM ma_stg_order o,
               ma_stg_order_option op,
               ma_stg_order_drops od,
               ma_stg_sizing_sku ms,
               ma_v_item_master im,
               ma_v_item_master im1,
               item_supplier mis,
               wh wh
         WHERE op.master_order_no= o.master_order_no
           AND op.option_id       = od.option_id
           AND o.master_order_no = od.master_order_no
           AND o.master_order_no = ms.master_order_no
           AND od.order_no       = ms.order_no
           AND mis.item           = im.item
           AND im1.item           = od.option_id
           AND mis.supplier       = o.supplier
           AND im.item            = ms.sku
           AND wh.wh              = od.first_dest
           AND o.master_order_no = I_master_order_no
           AND ms.qty_ordered IS NOT NULL)
    SELECT order_level,
           master_order_no, 
           option_id,
           option_desc, 
           --order_no,
           units,
           first_dest,
           first_dest_date, 
           supplier_size, 
           sku,
           sku_desc,
           po_type,
           supplier,
           manu_country_id,
           ship_method,
           fc_currency,
           fc_desc,
           freight_forward
      FROM (SELECT '1' order_level,
                   master_order_no, 
                   option_id, 
                   option_desc,
                   --order_no,
                   SUM(units) units,
                   first_dest,
                   first_dest_date, 
                   fc_desc,
                   NULL supplier_size, 
                   NULL sku,
                   NULL sku_desc,
                   NULL po_type,
                   NULL supplier,
                   NULL manu_country_id,
                   NULL ship_method,
                   NULL fc_currency,                   
                   NULL freight_forward
              FROM tab_skus t
             GROUP BY master_order_no, 
                      option_id, 
                      option_desc,
                      --order_no,
                      first_dest,
                      first_dest_date,
                      fc_desc
            UNION ALL
            SELECT '2' order_level,
                   master_order_no, 
                   option_id, 
                   option_desc,
                   --order_no,
                   units,
                   first_dest,
                   first_dest_date,
                   fc_desc, 
                   supplier_size, 
                   sku,
                   sku_desc,
                   po_type,
                   supplier,
                   manu_country_id,
                   ship_method,
                   fc_currency,                   
                   freight_forward
              FROM tab_skus t)
    ORDER BY master_order_no, 
             --order_no,
             order_level,
             option_id,
             sku;
  --
  rec_detail_cost C_detail_cost%ROWTYPE;
  --
BEGIN
  --
  --
  --
  DELETE ma_stg_cost_duty_detail
    WHERE master_order_no = I_master_order_no;
  --
  OPEN C_detail_cost;
  LOOP
    FETCH C_detail_cost INTO rec_detail_cost;
    EXIT WHEN C_detail_cost%NOTFOUND;    
    --
    L_order_no     := NULL;/*rec_detail_cost.drop_id;*/
    L_po_type  := rec_detail_cost.po_type;
    --
    L_total_expense := NULL;
    --
    IF rec_detail_cost.order_level <> '1' THEN
      --
      L_supplier           := rec_detail_cost.supplier;
      L_option_id          := rec_detail_cost.option_id;
      L_manu_country_id    := rec_detail_cost.manu_country_id;
      L_first_dest         := rec_detail_cost.first_dest;
      L_ship_method        := rec_detail_cost.ship_method;
      L_freight_forward    := rec_detail_cost.freight_forward;
      L_first_dest_date    := rec_detail_cost.first_dest_date;      
      --
      -- Get Commodity code
      --
      L_import_country := NULL;
      L_commodity      := NULL;
      L_av_rate := NULL;
      --
      OPEN C_import_country;
      FETCH C_import_country INTO L_import_country;
      CLOSE C_import_country;
      --
      OPEN C_commodity;
      FETCH C_commodity INTO L_commodity;
      CLOSE C_commodity;            
      --
      -- Check hts_tariff_treatment
      --      
      OPEN C_hts_tariff_treatment;
      FETCH C_hts_tariff_treatment INTO L_av_rate;
      CLOSE C_hts_tariff_treatment;
      --

     /* IF L_freight_currency <> rec_detail_cost.fc_currency THEN
        --
        L_freight_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                     I_currency_out    => L_freight_currency,
                                                     I_currency_in     => rec_detail_cost.fc_currency,
                                                     I_currency_value  => L_freight_cost);
        --
      END IF;*/

      --L_total_value := rec_detail_cost.units * 
      --4.  Total value =  ((units of SKU on PO *Unit cost for the option  converted to First destination local currency) + Expense for the option in  First destination local  currency )* Rate %

      -- Freight cost will only be calculated if the supplier shipping terms are in 'FOB' or 'EXW'.
      --
      IF L_terms IN ('FOB','EXW') THEN        
        --
        L_carton_fill_rate   := NULL;
        L_per_count          := NULL;
        L_freight_currency   := NULL;
        L_cost_component     := NULL;
        --
        OPEN C_freight_matrix;
        FETCH C_freight_matrix INTO L_cost_component,
                                    L_carton_fill_rate,
                                    L_per_count,
                                    L_freight_currency;
        CLOSE C_freight_matrix;
        --        
        L_freight_cost := (L_carton_fill_rate/L_per_count);
        --
        IF L_freight_currency <> rec_detail_cost.fc_currency THEN
          --
          L_freight_cost := CURRENCY_SQL.CONVERT_VALUE(I_cost_retail_ind => 'N',
                                                       I_currency_out    => L_freight_currency,
                                                       I_currency_in     => rec_detail_cost.fc_currency,
                                                       I_currency_value  => L_freight_cost);
          --
        END IF;
        --
        L_total_value := L_freight_cost * rec_detail_cost.units;
        --
        -- Insert Freight cost
        -- 
        INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  L_order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  L_cost_component,
                  L_freight_currency,
                  L_freight_cost,
                  L_total_value);
        --
      END IF;
      --
      OPEN C_sup_imp_attr;
      FETCH C_sup_imp_attr INTO L_partner_1,
                                L_partner_2;
      CLOSE C_sup_imp_attr;
      --
      -- The re-processing cost will only be calculated if the freight forwarder on PO = partner _1 on sup_import_attr
      --
      IF L_partner_1 = L_freight_forward THEN
        --
        L_cost_component := NULL;
        L_rate           := NULL;
        L_cost_currency  := NULL;
        --
        L_key_value_1    := L_supplier;
        L_module  := 'SUPP';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --        
        L_re_processing_cost := L_rate * rec_detail_cost.units;
        --
        -- Insert re-processing cost
        -- 
        INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  L_order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  L_cost_component,
                  L_cost_currency,
                  L_rate,
                  L_re_processing_cost);
      END IF;
      --
      -- The labelon cost will only be calculated if the partner_2 field in sup_import_attr table is not null for the supplier site on PO.
      --
      IF L_partner_2 IS NOT NULL THEN
        --
        L_cost_component := NULL;
        L_rate           := NULL;
        L_cost_currency  := NULL;
        --
        L_key_value_1    := L_partner_2;
        L_module  := 'PTNR';
        -- 
        OPEN C_exp_prof;
        FETCH C_exp_prof INTO L_cost_component,
                              L_rate,
                              L_cost_currency;                              
        CLOSE C_exp_prof;
        --        
        L_re_processing_cost := L_rate * rec_detail_cost.units;
        --
        -- Insert re-processing cost
        -- 
        INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  L_order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  rec_detail_cost.supplier_size, 
                  rec_detail_cost.sku,
                  L_cost_component,
                  L_cost_currency,
                  L_rate,
                  L_re_processing_cost);
        --
      END IF;
      --
    ELSE
      --
      -- Check terms
      --
      L_supplier  := rec_detail_cost.supplier;
      L_option_id := rec_detail_cost.option_id;
      --
      OPEN C_terms;
      FETCH C_terms INTO L_terms;
      CLOSE C_terms;
      --
      -- insert record for header
      --            
      INSERT INTO ma_stg_cost_expense_detail
                 (expense_id,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
           VALUES(ma_stg_cost_expense_detail_seq.nextval,
                  rec_detail_cost.order_level,
                  rec_detail_cost.master_order_no, 
                  L_order_no, 
                  rec_detail_cost.option_id,
                  rec_detail_cost.units,
                  rec_detail_cost.first_dest, 
                  rec_detail_cost.fc_desc,                   
                  NULL, 
                  NULL,
                  NULL,
                  NULL,
                  NULL,
                  NULL);
      --
    END IF;  
    --
  END LOOP;
  --
  CLOSE C_detail_cost;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN FUNCTION_ERROR THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_DUTY_DETAILS_COST',
                                              I_error_backtrace   => L_error_message);
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => I_master_order_no,
                                              I_error_key         => 'ERROR_CALC_DUTY_DETAILS_COST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_DUTY_DETAILS_COST;
--------------------------------------------------------------------------------
FUNCTION CALC_COST_SUMMARY(O_error_message    OUT VARCHAR2,
                           I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CALC_COST_SUMMARY';  
  --
  FUNCTION_ERROR            EXCEPTION;
  --
BEGIN
  --
  IF CALC_OPTION_DETAILS_COST(O_error_message,
                              I_master_order_no) = FALSE THEN
    --
    RETURN TRUE;
    --
  END IF;
  --
  IF CALC_UP_CHARGE_DETAILS_COST(O_error_message,
                                 I_master_order_no) = FALSE THEN
    --
    RETURN TRUE;
    --
  END IF;
  --
  IF CALC_DROP_DETAILS_COST(O_error_message,
                            I_master_order_no) = FALSE THEN
    --
    RETURN TRUE;
    --
  END IF;
  --
  IF CALC_EXPENSE_DETAILS_COST(O_error_message,
                               I_master_order_no) = FALSE THEN
    --
    RETURN TRUE;
    --
  END IF;
  --  
  /*IF CALC_DUTY_DETAILS_COST(O_error_message,
                            I_order_no) = FALSE THEN
    --
    RETURN TRUE;
    --
  END IF;*/
  --  
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_CALC_COST_SUMMARY',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CALC_COST_SUMMARY;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_ORDER(O_error_message OUT VARCHAR2,
                             I_order_tbl     IN  MA_STG_ORDER_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_ORDER';  
  --
BEGIN
  --
  INSERT INTO ma_stg_order
            (master_order_no,
             supplier,
             contract_no,
             comments,
             status,
             submitted_date,
             approved_by,
             approved_date,
             create_id,
             create_datetime,
             last_update_id,
             last_update_datetime)
     (SELECT tbl.master_order_no,
             tbl.supplier,
             tbl.contract_no,
             tbl.comments,
             tbl.status,
             tbl.submitted_date,
             tbl.approved_by,
             tbl.approved_date,
             tbl.create_id,
             tbl.create_datetime,
             tbl.last_update_id,
             tbl.last_update_datetime
        FROM table(I_order_tbl) tbl);
  --
  return true;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_ORDER',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_ORDER;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_ORDER_OPTION(O_error_message    OUT VARCHAR2,
                                    I_order_option_tbl IN  MA_STG_ORDER_OPTION_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_ORDER_OPTION';  
  --
BEGIN
  --
  INSERT INTO ma_stg_order_option
            (master_order_no,
             option_id,
             unit_cost,
             qty_ordered,
             seq_no,
             factory,
             manu_country_id,
             size_group,
             supplier_reference,
             supplier_colour,
             packing_method,
             factory_risk_rating,
             create_id,
             create_datetime,
             last_update_id,
             last_update_datetime)
     (SELECT tbl.master_order_no,
             tbl.option_id,
             tbl.unit_cost,
             tbl.qty_ordered,
             tbl.seq_no,
             tbl.factory,
             tbl.manu_country_id,
             tbl.size_group,
             tbl.supplier_reference,
             tbl.supplier_colour,
             tbl.packing_method,
             tbl.factory_risk_rating,
             tbl.create_id,
             tbl.create_datetime,
             tbl.last_update_id,
             tbl.last_update_datetime
        FROM table(I_order_option_tbl) tbl);
  --
  return true;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_ORDER_OPTION',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_ORDER_OPTION;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_ORDER_ITEM_DIST(O_error_message       OUT VARCHAR2,
                                       I_order_item_dist_tbl IN  MA_STG_ORDER_ITEM_DIST_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_ORDER_ITEM_DIST';  
  --
BEGIN
  --
  INSERT INTO ma_stg_order_item_dist
            (id_seq,
             master_order_no,
             po_type,
             option_id,
             first_dest,
             final_dest,
             handover_date,
             qty_ordered,
             unit_cost,
             ship_method,
             supplier_reference,
             ship_method_final_dest,
             ship_date,
             first_dest_date,
             not_before_date,
             not_after_date,
             final_dest_date,
             freight_forward,
             ship_port,
             del_port,
             week_no,
             ex_factory_date,
             create_id,
             create_datetime,
             last_update_id,
             last_update_datetime)
     (SELECT tbl.id_seq,
             tbl.master_order_no,
             tbl.po_type,
             tbl.option_id,
             tbl.first_dest,
             tbl.final_dest,
             tbl.handover_date,
             tbl.qty_ordered,
             tbl.unit_cost,
             tbl.ship_method,
             tbl.supplier_reference,
             tbl.ship_method_final_dest,
             tbl.ship_date,
             tbl.first_dest_date,
             tbl.not_before_date,
             tbl.not_after_date,
             tbl.final_dest_date,
             tbl.freight_forward,
             tbl.ship_port,
             tbl.del_port,
             tbl.week_no,
             tbl.ex_factory_date,
             tbl.create_id,
             tbl.create_datetime,
             tbl.last_update_id,
             tbl.last_update_datetime
        FROM table(I_order_item_dist_tbl) tbl);
  --
  return true;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_ORDER_ITEM_DIST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_ORDER_ITEM_DIST;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_ORDER_DROPS(O_error_message   OUT VARCHAR2,
                                   I_order_drops_tbl IN  MA_STG_ORDER_DROPS_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_ORDER_DROPS';  
  --
BEGIN
  --
  INSERT INTO ma_stg_order_drops
             (master_order_no, 
              order_no, 
              po_type, 
              option_id, 
              first_dest, 
              final_dest, 
              handover_date, 
              qty_ordered, 
              unit_cost, 
              ship_method, 
              ship_date, 
              first_dest_date, 
              not_before_date, 
              not_after_date, 
              final_dest_date, 
              freight_forward, 
              ship_port, 
              del_port, 
              supplier_reference, 
              ship_method_final_dest, 
              rev_no, 
              create_id, 
              create_datetime, 
              last_update_id, 
              last_update_datetime)
     (SELECT tbl.master_order_no, 
             tbl.order_no, 
             tbl.po_type, 
             tbl.option_id, 
             tbl.first_dest, 
             tbl.final_dest, 
             tbl.handover_date, 
             tbl.qty_ordered, 
             tbl.unit_cost, 
             tbl.ship_method, 
             tbl.ship_date, 
             tbl.first_dest_date, 
             tbl.not_before_date, 
             tbl.not_after_date, 
             tbl.final_dest_date, 
             tbl.freight_forward, 
             tbl.ship_port, 
             tbl.del_port, 
             tbl.supplier_reference, 
             tbl.ship_method_final_dest, 
             tbl.rev_no, 
             tbl.create_id, 
             tbl.create_datetime, 
             tbl.last_update_id, 
             tbl.last_update_datetime
        FROM TABLE(I_order_drops_tbl) tbl);
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_ORDER_DROPS',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_ORDER_DROPS;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_ORDER_DROPS_DTL(O_error_message          OUT VARCHAR2,
                                       I_order_drops_detail_tbl IN  MA_STG_ORDER_DROPS_DETAIL_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_ORDER_DROPS_DTL';  
  --
BEGIN
  --
  INSERT INTO ma_stg_order_drops_detail
             (master_order_no, 
              order_no, 
              po_type, 
              option_id, 
              first_dest, 
              final_dest, 
              handover_date, 
              qty_ordered, 
              unit_cost, 
              ship_method, 
              ship_date, 
              first_dest_date, 
              not_before_date, 
              not_after_date, 
              final_dest_date, 
              freight_forward, 
              ship_port, 
              del_port, 
              supplier_reference, 
              ship_method_final_dest, 
              seq_no, 
              create_id, 
              create_datetime, 
              last_update_id, 
              last_update_datetime)
      (SELECT tbl.master_order_no, 
              tbl.order_no, 
              tbl.po_type, 
              tbl.option_id, 
              tbl.first_dest, 
              tbl.final_dest, 
              tbl.handover_date, 
              tbl.qty_ordered, 
              tbl.unit_cost, 
              tbl.ship_method, 
              tbl.ship_date, 
              tbl.first_dest_date, 
              tbl.not_before_date, 
              tbl.not_after_date, 
              tbl.final_dest_date, 
              tbl.freight_forward, 
              tbl.ship_port, 
              tbl.del_port, 
              tbl.supplier_reference, 
              tbl.ship_method_final_dest, 
              tbl.seq_no, 
              tbl.create_id, 
              tbl.create_datetime, 
              tbl.last_update_id, 
              tbl.last_update_datetime
        FROM TABLE(I_order_drops_detail_tbl) tbl);
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_ORDER_DROPS_DTL',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_ORDER_DROPS_DTL;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_SIZING_OPTION_DIST(O_error_message          OUT VARCHAR2,
                                      I_sizing_option_dist_tbl IN  MA_STG_SIZING_OPTION_DIST_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_SIZING_OPTION_DIST';  
  --
BEGIN
  --
  INSERT INTO ma_stg_sizing_option_dist
            (seq_no,
             master_order_no,
             order_no,
             option_id,
             final_dest,
             exp_delivery_date,
             qty_ordered,
             sizing_applied,
             size_profile,
             size_group,
             distributed_by,
             supplier_reference,
             create_id,
             create_datetime,
             last_update_id,
             last_update_datetime)
     (SELECT tbl.seq_no,
             tbl.master_order_no,
             tbl.order_no,
             tbl.option_id,
             tbl.final_dest,
             tbl.exp_delivery_date,
             tbl.qty_ordered,
             tbl.sizing_applied,
             tbl.size_profile,
             tbl.size_group,
             tbl.distributed_by,
             tbl.supplier_reference,
             tbl.create_id,
             tbl.create_datetime,
             tbl.last_update_id,
             tbl.last_update_datetime
        FROM table(I_sizing_option_dist_tbl) tbl);
  --
  return true;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_SIZING_OPTION_DIST',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_SIZING_OPTION_DIST;
--------------------------------------------------------------------------------
FUNCTION INSERT_MA_STG_SIZING_SKU(O_error_message          OUT VARCHAR2,
                                  I_sizing_sku_tbl IN  MA_STG_SIZING_SKU_TBL)
RETURN BOOLEAN IS
  --
  L_program     VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.INSERT_MA_STG_SIZING_SKU';  
  --
BEGIN
  --
  INSERT INTO ma_stg_sizing_sku
            (seq_no,
             master_order_no,
             order_no,
             option_id,
             final_dest,
             exp_delivery_date,
             sku,
             size_code,
             percentage,
             ratio,
             qty_ordered,
             create_id,
             create_datetime,
             last_update_id,
             last_update_datetime)
     (SELECT tbl.seq_no,
             tbl.master_order_no,
             tbl.order_no,
             tbl.option_id,
             tbl.final_dest,
             tbl.exp_delivery_date,
             tbl.sku,
             tbl.size_code,
             tbl.percentage,
             tbl.ratio,
             tbl.qty_ordered,
             tbl.create_id,
             tbl.create_datetime,
             tbl.last_update_id,
             tbl.last_update_datetime
        FROM table(I_sizing_sku_tbl) tbl);
  --
  return true;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => NULL,
                                              I_error_key         => 'INSERT_MA_STG_SIZING_SKU',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END INSERT_MA_STG_SIZING_SKU;
--------------------------------------------------------------------------------
FUNCTION CREATE_ORDER_REC_PLANNING (O_error_message    OUT VARCHAR2,
                                    I_order_rec_no_ids IN  VARCHAR2,
                                    O_master_order_no  OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_ORDER_REC_PLANNING';  
  PROGRAM_ERROR            EXCEPTION;
  --
  L_order_tbl              MA_STG_ORDER_TBL;
  L_order_obj              ma_stg_order_obj;
  L_order_option_tbl       MA_STG_ORDER_OPTION_TBL;
  L_order_option_obj       ma_stg_order_option_obj;
  L_order_item_dist_tbl    MA_STG_ORDER_ITEM_DIST_TBL;
  L_order_item_dist_obj    ma_stg_order_item_dist_obj;
  L_sizing_option_dist_tbl MA_STG_SIZING_OPTION_DIST_TBL;
  L_sizing_option_dist_obj ma_stg_sizing_option_dist_obj;
  L_sizing_sku_tbl         MA_STG_SIZING_SKU_TBL;
  L_sizing_sku_obj         ma_stg_sizing_sku_obj;
  --
  L_order_no               MA_STG_ORDER.MASTER_ORDER_NO%TYPE;
  L_order_rec_no           MA_ORDER_REC_HEAD_STG.ORDER_REC_NO%TYPE;
  L_dummy                  NUMBER;
  L_wks_status             MA_STG_ORDER.STATUS%TYPE := 'W';
  L_po_type                MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE := 'D';
  L_supplier               MA_STG_ORDER.SUPPLIER%TYPE;
  L_audit_date             DATE := SYSDATE;
  L_order_option_seq       NUMBER := 0;
  L_ship_method            MA_STG_ORDER_ITEM_DIST.SHIP_METHOD%TYPE;
  L_ship_method_final_dest MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE;
  L_ship_date              MA_STG_ORDER_ITEM_DIST.SHIP_DATE%TYPE;
  L_handover_date          MA_STG_ORDER_ITEM_DIST.HANDOVER_DATE%TYPE;
  L_first_dest_date        MA_STG_ORDER_ITEM_DIST.FIRST_DEST_DATE%TYPE;
  L_not_before_date        MA_STG_ORDER_ITEM_DIST.NOT_BEFORE_DATE%TYPE;
  L_not_after_date         MA_STG_ORDER_ITEM_DIST.NOT_AFTER_DATE%TYPE;
  L_final_dest_date        MA_STG_ORDER_ITEM_DIST.FINAL_DEST_DATE%TYPE;
  L_freight_forward        MA_STG_ORDER_ITEM_DIST.FREIGHT_FORWARD%TYPE;
  L_ship_port              MA_STG_ORDER_ITEM_DIST.SHIP_PORT%TYPE;
  L_del_port               MA_STG_ORDER_ITEM_DIST.DEL_PORT%TYPE;
  L_week_no                MA_STG_ORDER_ITEM_DIST.WEEK_NO%TYPE;
  L_ex_factory_date        MA_STG_ORDER_ITEM_DIST.EX_FACTORY_DATE%TYPE;
  L_percentage             MA_STG_SIZING_SKU.PERCENTAGE%TYPE := NULL;
  L_ratio                  MA_STG_SIZING_SKU.RATIO%TYPE      := NULL;
  L_exp_delivery_date      MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE;
  L_idx                    NUMBER;
  L_seq_no                 NUMBER; 
  --
  CURSOR C_check IS
    SELECT COUNT(1) over (PARTITION BY 1) dummy,
           supplier
      FROM (SELECT DISTINCT
                   supplier,
                   mb.buying_group,
                   mb.business_model
              FROM ma_order_rec_head_stg h,
                   (SELECT to_number(column_value) order_rec_no
                      FROM TABLE(convert_comma_list(I_order_rec_no_ids))
                   ) od,
                   ma_v_buyerarchy mb
             WHERE h.order_rec_no = od.order_rec_no
               AND mb.item        = h.option_id
           );
  --
  CURSOR C_planning_head IS
    SELECT h.order_rec_no, 
           option_id,
           diff_2, 
           vpn,
           supp_diff_1,
           packing_method,
           qty_ordered, 
           h.supplier, 
           unit_cost, 
           final_dest, 
           loc_type, 
           origin_country_id,
           size_profile, 
           handover_date, 
           not_before_date, 
           not_after_date,
           ms.ship_method,
           ms.ship_port,
           ms.del_port,
           ms.ff freight_forward
      FROM ma_order_rec_head_stg h,
           (SELECT column_value order_rec_no
              FROM TABLE(convert_comma_list(I_order_rec_no_ids))
           ) od,
           (select distinct
             supplier,
             item,
             diff_2, 
             vpn,
             supp_diff_1,
             primary_supp_ind,
             packing_method,
             unit_cost,
             origin_country_id
             from ma_v_option_supplier) os,
           ma_v_ship ms
     WHERE h.order_rec_no   = od.order_rec_no
       AND os.item          = h.option_id
       AND os.supplier      = h.supplier
       AND primary_supp_ind = 'Y'
       AND ms.supplier      = h.supplier;
  --
  CURSOR C_planning_dtl IS
    SELECT sku_id,
           size_code,
           qty_ordered 
      FROM ma_order_rec_detail_stg
     WHERE order_rec_no = L_order_rec_no;
  --
BEGIN
  --
  -- Check if supplier is the same and Buying group for the option is also the same
  --
  OPEN C_check;
  FETCH C_check INTO L_dummy,
                     L_supplier;
  CLOSE C_check;
  --
  IF L_dummy > 1 THEN
    --
    O_error_message := 'DIFF_SUPP_WARN';
    RAISE PROGRAM_ERROR;
    --
  END IF;
  --
  -- Insert Orders
  --
  O_master_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
  --
  -- insert record into ma_stg_order table
  --
  L_order_obj := ma_stg_order_obj(master_order_no      => O_master_order_no,
                                  supplier             => L_supplier,
                                  contract_no          => NULL,
                                  comments             => NULL,
                                  status               => L_wks_status,
                                  submitted_date       => NULL,
                                  approved_by          => NULL,
                                  approved_date        => NULL,
                                  create_id            => get_app_user,
                                  create_datetime      => L_audit_date,
                                  last_update_id       => get_app_user,
                                  last_update_datetime => L_audit_date);

  L_order_tbl := MA_STG_ORDER_TBL();
  L_order_tbl.extend();
  L_order_tbl(L_order_tbl.count) := L_order_obj;
  --         
  IF INSERT_MA_STG_ORDER(O_error_message,
                         L_order_tbl) = FALSE THEN
    --
    RETURN FALSE;
    --                    
  END IF;
  -- 
  -- insert record into ma_stg_order_option table
  --
  INSERT INTO ma_stg_order_option
          (master_order_no, 
           option_id, 
           unit_cost, 
           qty_ordered, 
           seq_no, 
           factory, 
           manu_country_id, 
           size_group, 
           supplier_reference, 
           supplier_colour, 
           packing_method, 
           factory_risk_rating, 
           create_id, 
           create_datetime, 
           last_update_id, 
           last_update_datetime)
    SELECT master_order_no, 
           option_id, 
           unit_cost, 
           qty_ordered, 
           rownum seq_no, 
           factory, 
           manu_country_id, 
           size_group, 
           supplier_reference, 
           supplier_colour, 
           packing_method, 
           factory_risk_rating, 
           create_id, 
           create_datetime, 
           last_update_id, 
           last_update_datetime
      FROM (SELECT O_master_order_no   master_order_no,
                   option_id,
                   unit_cost, 
                   SUM(qty_ordered)    qty_ordered, 
                   NULL                factory,
                   origin_country_id   manu_country_id,
                   diff_2              size_group, 
                   vpn                 supplier_reference,
                   supp_diff_1         supplier_colour,
                   packing_method,
                   NULL                factory_risk_rating,
                   get_app_user                create_id,
                   L_audit_date        create_datetime,
                   get_app_user                last_update_id,
                   L_audit_date        last_update_datetime
              FROM ma_order_rec_head_stg h,
                   (SELECT column_value order_rec_no
                      FROM TABLE(convert_comma_list(I_order_rec_no_ids))
                   ) od,
                   (select distinct
                       supplier,
                       item,
                       unit_cost,
                       origin_country_id,
                       diff_2,
                       vpn,
                       supp_diff_1,
                       packing_method,
                       primary_supp_ind
                     from ma_v_option_supplier) os,
                   ma_v_ship ms
             WHERE h.order_rec_no   = od.order_rec_no
               AND os.item          = h.option_id
               AND os.supplier      = h.supplier
               AND primary_supp_ind = 'Y'
               AND ms.supplier      = h.supplier
            GROUP BY O_master_order_no,
                     option_id,
                     unit_cost,
                     NULL,
                     origin_country_id,
                     diff_2, 
                     vpn,
                     supp_diff_1,
                     packing_method,
                     get_app_user,
                     L_audit_date,
                     get_app_user,
                     L_audit_date
          );
  --
  FOR C_rec IN C_planning_head LOOP
    --
    L_order_rec_no := C_rec.order_rec_no;
    /*
    --
    -- insert record into ma_stg_order_option table
    --
    L_order_option_seq := L_order_option_seq + 1;
    --
    L_order_option_obj := ma_stg_order_option_obj(master_order_no      => O_master_order_no,
                                                  option_id            => C_rec.option_id,
                                                  unit_cost            => C_rec.unit_cost,
                                                  qty_ordered          => C_rec.qty_ordered,
                                                  seq_no               => L_order_option_seq,
                                                  factory              => NULL,
                                                  manu_country_id      => C_rec.origin_country_id,
                                                  size_group           => C_rec.diff_2,
                                                  supplier_reference   => C_rec.vpn,
                                                  supplier_colour      => C_rec.supp_diff_1,
                                                  packing_method       => C_rec.packing_method,
                                                  factory_risk_rating  => C_rec.factory_risk_rating,
                                                  create_id            => get_app_user,
                                                  create_datetime      => L_audit_date,
                                                  last_update_id       => get_app_user,
                                                  last_update_datetime => L_audit_date);
    --
    L_order_option_tbl := MA_STG_ORDER_OPTION_TBL();
    L_order_option_tbl.extend();
    L_order_option_tbl(L_order_option_tbl.count) := L_order_option_obj;
    --
    IF INSERT_MA_STG_ORDER_OPTION(O_error_message,
                                  L_order_option_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
    L_order_option_tbl.delete;
    */
    --
    -- insert record into ma_stg_order_item_dist table
    --
    --
    -- Get dates
    --
    L_handover_date          := C_rec.handover_date;
    L_not_before_date        := C_rec.not_before_date;
    L_ship_method            := C_rec.ship_method;
    L_freight_forward        := C_rec.freight_forward;
    L_ship_port              := C_rec.ship_port;
    L_del_port               := C_rec.del_port;
    L_ship_method_final_dest := L_ship_method;
    L_seq_no                 := ma_stg_order_item_dist_seq.nextval;
    --
    IF MA_ORDER_UTILS_SQL.GET_DATE (O_error_message,
                                    NULL,
                                    L_po_type,
                                    L_ship_port,
                                    C_rec.final_dest, 
                                    L_ship_method, 
                                    L_freight_forward, 
                                    C_rec.final_dest, 
                                    L_ship_method_final_dest,             
                                    L_handover_date,
                                    L_ship_date,
                                    L_not_before_date,
                                    L_not_after_date,
                                    L_first_dest_date,  
                                    L_final_dest_date,
                                    L_ex_factory_date,
                                    L_week_no) = FALSE THEN
      --
      RETURN FALSE;
      --
    END IF;
    --
    L_order_item_dist_obj := ma_stg_order_item_dist_obj(id_seq                 => L_seq_no, --L_order_option_seq,
                                                        master_order_no        => O_master_order_no,
                                                        po_type                => L_po_type,
                                                        option_id              => C_rec.option_id,
                                                        first_dest             => C_rec.final_dest,
                                                        final_dest             => C_rec.final_dest,
                                                        handover_date          => L_handover_date, --NVL(C_rec.handover_date, C_rec.not_before_date)
                                                        qty_ordered            => C_rec.qty_ordered,
                                                        unit_cost              => C_rec.unit_cost,
                                                        ship_method            => L_ship_method,
                                                        supplier_reference     => C_rec.vpn,
                                                        ship_method_final_dest => L_ship_method_final_dest,
                                                        ship_date              => L_ship_date,
                                                        first_dest_date        => L_first_dest_date,
                                                        not_before_date        => NVL(L_not_before_date, C_rec.not_before_date),
                                                        not_after_date         => NVL(L_not_after_date, C_rec.not_after_date),
                                                        final_dest_date        => L_final_dest_date,
                                                        freight_forward        => L_freight_forward,
                                                        ship_port              => L_ship_port,
                                                        del_port               => L_del_port,
                                                        week_no                => L_week_no,
                                                        ex_factory_date        => L_ex_factory_date,
                                                        create_id              => get_app_user,
                                                        create_datetime        => L_audit_date,
                                                        last_update_id         => get_app_user,
                                                        last_update_datetime   => L_audit_date);
    --
    L_order_item_dist_tbl := MA_STG_ORDER_ITEM_DIST_TBL();
    L_order_item_dist_tbl.extend();
    L_order_item_dist_tbl(L_order_item_dist_tbl.count) := L_order_item_dist_obj;
    --
    IF INSERT_MA_STG_ORDER_ITEM_DIST(O_error_message,
                                     L_order_item_dist_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
    L_order_item_dist_tbl.delete;
    --
    -- insert record into ma_stg_sizing_option_dist table
    --
    L_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
    --
    L_sizing_option_dist_obj := MA_STG_SIZING_OPTION_DIST_OBJ(seq_no               => L_seq_no,
                                                              master_order_no      => O_master_order_no,
                                                              order_no             => L_order_no,
                                                              option_id            => C_rec.option_id,
                                                              final_dest           => C_rec.final_dest,
                                                              exp_delivery_date    => NVL(L_handover_date, C_rec.not_before_date),
                                                              qty_ordered          => C_rec.qty_ordered,
                                                              sizing_applied       => 'Y',
                                                              size_profile         => C_rec.size_profile,
                                                              size_group           => C_rec.diff_2,
                                                              distributed_by       => 'Q',
                                                              supplier_reference   => C_rec.vpn,
                                                              create_id            => get_app_user,
                                                              create_datetime      => L_audit_date,
                                                              last_update_id       => get_app_user,
                                                              last_update_datetime => L_audit_date);
    --
    L_sizing_option_dist_tbl := MA_STG_SIZING_OPTION_DIST_TBL();
    L_sizing_option_dist_tbl.extend();
    L_sizing_option_dist_tbl(L_sizing_option_dist_tbl.count) := L_sizing_option_dist_obj;
    --
    IF INSERT_MA_SIZING_OPTION_DIST(O_error_message,
                                    L_sizing_option_dist_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
    L_sizing_option_dist_tbl.delete;
    --
    -- insert record into ma_stg_sizing_sku table
    --
    L_sizing_sku_tbl := MA_STG_SIZING_SKU_TBL();
    --
    FOR C_rec_dtl IN C_planning_dtl LOOP
      --
      L_sizing_sku_obj := MA_STG_SIZING_SKU_OBJ(seq_no               => L_seq_no,
                                                master_order_no      => O_master_order_no,
                                                order_no             => L_order_no,
                                                option_id            => C_rec.option_id,
                                                final_dest           => C_rec.final_dest,
                                                exp_delivery_date    => NVL(L_handover_date, C_rec.not_before_date),
                                                sku                  => C_rec_dtl.sku_id,
                                                size_code            => C_rec_dtl.size_code,
                                                percentage           => L_percentage,
                                                ratio                => L_ratio,
                                                qty_ordered          => C_rec_dtl.qty_ordered,
                                                create_id            => get_app_user,
                                                create_datetime      => L_audit_date,
                                                last_update_id       => get_app_user,
                                                last_update_datetime => L_audit_date);
      --
      L_sizing_sku_tbl.extend();
      L_sizing_sku_tbl(L_sizing_sku_tbl.count) := L_sizing_sku_obj;
      --
    END LOOP;
    --
    IF INSERT_MA_STG_SIZING_SKU(O_error_message,
                                L_sizing_sku_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
    -- Update sizing_applied ma_stg_sizing_option_dist if doesn't exist sizing sku for master_order
    --
    UPDATE ma_stg_sizing_option_dist
      SET sizing_applied = 'N'
     WHERE order_no = L_order_no
       AND NOT EXISTS (SELECT 1
                         FROM ma_stg_sizing_sku
                        WHERE order_no = L_order_no
                       );
    --
    L_sizing_sku_tbl.delete;
    --
    -- Insert record into ma_stg_order_rec table
    --
    INSERT INTO ma_stg_order_rec
            (master_order_no, 
             rec_plan_no, 
             rec_rpl_no)
      VALUES(O_master_order_no,
             L_order_rec_no,
             NULL);
    --
  END LOOP;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => L_order_rec_no,
                                              I_error_key         => 'CREATE_ORDER_REC_PLANNING',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_ORDER_REC_PLANNING;
--------------------------------------------------------------------------------
FUNCTION CREATE_ORDER_REC_REPLENISHMENT (O_error_message            OUT VARCHAR2,
                                         I_ma_order_rec_rpl_lst_tbl IN  MA_ORDER_REC_RPL_LST_TBL,
                                         O_master_order_no          OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CREATE_ORDER_REC_PLANNING';  
  PROGRAM_ERROR            EXCEPTION;
  --
  L_order_tbl              MA_STG_ORDER_TBL;
  L_order_obj              ma_stg_order_obj;
  L_order_option_tbl       MA_STG_ORDER_OPTION_TBL;
  L_order_option_obj       ma_stg_order_option_obj;
  L_order_item_dist_tbl    MA_STG_ORDER_ITEM_DIST_TBL;
  L_order_item_dist_obj    ma_stg_order_item_dist_obj;
  L_sizing_option_dist_tbl MA_STG_SIZING_OPTION_DIST_TBL;
  L_sizing_option_dist_obj ma_stg_sizing_option_dist_obj;
  L_sizing_sku_tbl         MA_STG_SIZING_SKU_TBL;
  L_sizing_sku_obj         ma_stg_sizing_sku_obj;
  --
  L_order_no               MA_STG_ORDER.MASTER_ORDER_NO%TYPE;
  L_order_rec_no           MA_STG_ORDER_REC_RPL.ORDER_REC_NO%TYPE;
  L_dummy                  NUMBER;
  L_wks_status             MA_STG_ORDER.STATUS%TYPE := 'W';
  L_po_type                MA_STG_ORDER_ITEM_DIST.PO_TYPE%TYPE := 'D';
  L_supplier               MA_STG_ORDER.SUPPLIER%TYPE;
  L_audit_date             DATE := SYSDATE;
  L_order_option_seq       NUMBER := 0;
  L_ship_method            MA_STG_ORDER_ITEM_DIST.SHIP_METHOD%TYPE;
  L_ship_method_final_dest MA_STG_ORDER_ITEM_DIST.SHIP_METHOD_FINAL_DEST%TYPE;
  L_ship_date              MA_STG_ORDER_ITEM_DIST.SHIP_DATE%TYPE;
  L_handover_date          MA_STG_ORDER_ITEM_DIST.HANDOVER_DATE%TYPE;
  L_first_dest_date        MA_STG_ORDER_ITEM_DIST.FIRST_DEST_DATE%TYPE;
  L_not_before_date        MA_STG_ORDER_ITEM_DIST.NOT_BEFORE_DATE%TYPE;
  L_not_after_date         MA_STG_ORDER_ITEM_DIST.NOT_AFTER_DATE%TYPE;
  L_final_dest_date        MA_STG_ORDER_ITEM_DIST.FINAL_DEST_DATE%TYPE;
  L_freight_forward        MA_STG_ORDER_ITEM_DIST.FREIGHT_FORWARD%TYPE;
  L_ship_port              MA_STG_ORDER_ITEM_DIST.SHIP_PORT%TYPE;
  L_del_port               MA_STG_ORDER_ITEM_DIST.DEL_PORT%TYPE;
  L_week_no                MA_STG_ORDER_ITEM_DIST.WEEK_NO%TYPE;
  L_ex_factory_date        MA_STG_ORDER_ITEM_DIST.EX_FACTORY_DATE%TYPE;
  L_percentage             MA_STG_SIZING_SKU.PERCENTAGE%TYPE := NULL;
  L_ratio                  MA_STG_SIZING_SKU.RATIO%TYPE      := NULL;
  L_exp_delivery_date      MA_STG_SIZING_SKU.EXP_DELIVERY_DATE%TYPE;
  L_idx                    NUMBER;
  L_seq_no                 NUMBER; 
  L_option_id              MA_STG_ORDER_ITEM_DIST.OPTION_ID%TYPE;
  L_qty_ordered            MA_STG_ORDER_ITEM_DIST.QTY_ORDERED%TYPE;
  --
  CURSOR C_check IS
    SELECT COUNT(1) over (PARTITION BY 1) dummy,
           supplier
      FROM (SELECT DISTINCT
                   supplier,
                   mb.buying_group,
                   mb.business_model
              FROM ma_v_replenishment r,
                   (SELECT sku_id, 
                           final_dest, 
                           exp_delivery_date
                      FROM TABLE(I_ma_order_rec_rpl_lst_tbl)
                   ) obj,
                   ma_v_buyerarchy mb
             WHERE r.item      = obj.sku_id
               AND r.location  = obj.final_dest
               AND r.need_date = obj.exp_delivery_date
               AND mb.item     = r.item
           ); 
  --
  CURSOR C_replenishment_head IS
    SELECT L_order_rec_no order_rec_no, 
           r.order_level,
           r.parent option_id,
           r.item sku_id,
           r.size_code,
           diff_2, 
           vpn,
           supp_diff_1,
           packing_method,
           qty_ordered, 
           r.supplier, 
           --factory_risk_rating,
           unit_cost, 
           final_dest, 
           loc_type, 
           origin_country_id,
           NULL size_profile,
           need_date,
           ms.ship_method,
           ms.ship_port,
           ms.del_port,
           ms.ff freight_forward
      FROM ma_v_replenishment r,
           (SELECT sku_id, 
                   final_dest, 
                   exp_delivery_date
              FROM TABLE(I_ma_order_rec_rpl_lst_tbl)
           ) obj,
           (select distinct
              supplier,
              item,
              vpn,
              supp_diff_1,
              diff_2, 
              unit_cost,
              factory,
              packing_method,
              primary_supp_ind,
              origin_country_id
            from ma_v_option_supplier) os,
           --ma_v_supplier_factory sf,
           ma_v_ship ms
     WHERE r.item           = obj.sku_id
       AND r.location       = obj.final_dest
       AND r.need_date      = obj.exp_delivery_date
       AND os.item          = r.parent
       AND os.supplier      = r.supplier
       AND primary_supp_ind = 'Y'
       --AND sf.supplier      = os.supplier
       --AND sf.factory       = os.factory
       AND ms.supplier      = r.supplier
   ORDER BY r.parent, 
            order_level;
  --
  CURSOR C_sum_qty is
    SELECT SUM(qty_ordered)
      FROM ma_v_replenishment r,
           (SELECT sku_id, 
                   final_dest, 
                   exp_delivery_date
              FROM TABLE(I_ma_order_rec_rpl_lst_tbl)
           ) obj
     WHERE r.item           = obj.sku_id
       AND r.location       = obj.final_dest
       AND r.need_date      = obj.exp_delivery_date
       AND r.parent         = L_option_id;
BEGIN
  --
  -- Check if supplier is the same and Buying group for the option is also the same
  --
  SAVEPOINT INIT_REPL_REC;

  OPEN C_check;
  FETCH C_check INTO L_dummy,
                     L_supplier;
  CLOSE C_check;
  --
  IF L_dummy > 1 THEN
    --
    O_error_message := 'DIFF_SUPP_WARN';
    RAISE PROGRAM_ERROR;
    --
  END IF;
  --
  L_order_rec_no := ma_order_rec_rpl_seq.nextval;
  O_master_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
  --
  -- Populate replanishment staging table 
  --
  BEGIN
    --
          --
      -- Insert record into ma_stg_order_rec table
      --
      INSERT INTO ma_stg_order_rec
              (master_order_no, 
               rec_plan_no, 
               rec_rpl_no)
        VALUES(O_master_order_no,
               NULL,
               L_order_rec_no);
      --
      --
    INSERT INTO ma_stg_order_rec_rpl
             (order_rec_no, 
              option_id, 
              sku_id, 
              final_dest, 
              exp_delivery_date, 
              create_id, 
              create_datetime, 
              last_update_id, 
              last_update_datetime)
       SELECT L_order_rec_no,
              r.parent,
              r.item,
              r.location,
              r.need_date,
              get_app_user,
              L_audit_date,
              get_app_user,
              L_audit_date
         FROM ma_v_replenishment r,
              (SELECT sku_id, 
                     final_dest, 
                     exp_delivery_date
                FROM TABLE(I_ma_order_rec_rpl_lst_tbl)
              ) obj
        WHERE r.item           = obj.sku_id
          AND r.location       = obj.final_dest
          AND r.need_date      = obj.exp_delivery_date;
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN
      O_error_message := 'REC_IN_USE';
      RETURN FALSE;


  END;
  --
  -- Insert Orders
  --

  --
  -- insert record into ma_stg_order table
  --
  L_order_obj := ma_stg_order_obj(master_order_no      => O_master_order_no,
                                  supplier             => L_supplier,
                                  contract_no          => NULL,
                                  comments             => NULL,
                                  status               => L_wks_status,
                                  submitted_date       => NULL,
                                  approved_by          => NULL,
                                  approved_date        => NULL,
                                  create_id            => get_app_user,
                                  create_datetime      => L_audit_date,
                                  last_update_id       => get_app_user,
                                  last_update_datetime => L_audit_date);

  L_order_tbl := MA_STG_ORDER_TBL();
  L_order_tbl.extend();
  L_order_tbl(L_order_tbl.count) := L_order_obj;
  --       
  IF INSERT_MA_STG_ORDER(O_error_message,
                         L_order_tbl) = FALSE THEN
    --
    ROLLBACK TO INIT_REPL_REC;
    RETURN FALSE;
    --                    
  END IF;
  -- 
  L_option_id := NULL;
  --
  FOR C_rec IN C_replenishment_head LOOP
    --
    L_order_rec_no := C_rec.order_rec_no;
    --
    -- insert record into ma_stg_order_option table
    --
    IF NVL(L_option_id, '-ZZZ') <> C_rec.option_id THEN
      --
      L_option_id := C_rec.option_id;
      --
      OPEN C_sum_qty;
      FETCH C_sum_qty INTO L_qty_ordered;
      CLOSE C_sum_qty;
      --
      L_order_option_seq := L_order_option_seq + 1;
      --
      L_order_option_obj := ma_stg_order_option_obj(master_order_no      => O_master_order_no,
                                                    option_id            => C_rec.option_id,
                                                    unit_cost            => C_rec.unit_cost,
                                                    qty_ordered          => L_qty_ordered,
                                                    seq_no               => L_order_option_seq,
                                                    factory              => NULL,
                                                    manu_country_id      => C_rec.origin_country_id,
                                                    size_group           => C_rec.diff_2,
                                                    supplier_reference   => C_rec.vpn,
                                                    supplier_colour      => C_rec.supp_diff_1,
                                                    packing_method       => C_rec.packing_method,
                                                    factory_risk_rating  => NULL,
                                                    create_id            => get_app_user,
                                                    create_datetime      => L_audit_date,
                                                    last_update_id       => get_app_user,
                                                    last_update_datetime => L_audit_date);
      --
      L_order_option_tbl := MA_STG_ORDER_OPTION_TBL();
      L_order_option_tbl.extend();
      L_order_option_tbl(L_order_option_tbl.count) := L_order_option_obj;
      --
      IF INSERT_MA_STG_ORDER_OPTION(O_error_message,
                                    L_order_option_tbl) = FALSE THEN
        --
        ROLLBACK TO INIT_REPL_REC;
        RETURN FALSE;
        --                    
      END IF;
      --
      L_order_option_tbl.delete;
      --
      -- insert record into ma_stg_order_item_dist table
      --
      --
      -- Get dates
      --
      L_handover_date          := NULL;
      L_not_before_date        := NULL;
      L_ship_method            := C_rec.ship_method;
      L_freight_forward        := C_rec.freight_forward;
      L_ship_port              := C_rec.ship_port;
      L_del_port               := C_rec.del_port;
      L_ship_method_final_dest := L_ship_method;
      L_first_dest_date        := C_rec.need_date;
      L_final_dest_date        := NULL;
      L_not_after_date         := NULL;
      L_seq_no                 := ma_stg_order_item_dist_seq.nextval;
      --
      IF MA_ORDER_UTILS_SQL.GET_DATE (O_error_message,
                                      NULL,
                                      L_po_type,
                                      L_ship_port,
                                      C_rec.final_dest, 
                                      L_ship_method, 
                                      L_freight_forward, 
                                      C_rec.final_dest, 
                                      L_ship_method_final_dest,             
                                      L_handover_date,
                                      L_ship_date,
                                      L_not_before_date,
                                      L_not_after_date,
                                      L_first_dest_date,  
                                      L_final_dest_date,
                                      L_ex_factory_date,
                                      L_week_no) = FALSE THEN
        --
        ROLLBACK TO INIT_REPL_REC;
        RETURN FALSE;
        --
      END IF;
      --
      L_order_item_dist_obj := ma_stg_order_item_dist_obj(id_seq                 => L_seq_no, 
                                                          master_order_no        => O_master_order_no,
                                                          po_type                => L_po_type,
                                                          option_id              => C_rec.option_id,
                                                          first_dest             => C_rec.final_dest,
                                                          final_dest             => C_rec.final_dest,
                                                          handover_date          => L_handover_date,
                                                          qty_ordered            => L_qty_ordered,
                                                          unit_cost              => C_rec.unit_cost,
                                                          ship_method            => L_ship_method,
                                                          supplier_reference     => C_rec.vpn,
                                                          ship_method_final_dest => L_ship_method_final_dest,
                                                          ship_date              => L_ship_date,
                                                          first_dest_date        => L_first_dest_date,
                                                          not_before_date        => L_not_before_date,
                                                          not_after_date         => L_not_after_date,
                                                          final_dest_date        => L_final_dest_date,
                                                          freight_forward        => L_freight_forward,
                                                          ship_port              => L_ship_port,
                                                          del_port               => L_del_port,
                                                          week_no                => L_week_no,
                                                          ex_factory_date        => L_ex_factory_date,
                                                          create_id              => get_app_user,
                                                          create_datetime        => L_audit_date,
                                                          last_update_id         => get_app_user,
                                                          last_update_datetime   => L_audit_date);
      --
      L_order_item_dist_tbl := MA_STG_ORDER_ITEM_DIST_TBL();
      L_order_item_dist_tbl.extend();
      L_order_item_dist_tbl(L_order_item_dist_tbl.count) := L_order_item_dist_obj;
      --
      IF INSERT_MA_STG_ORDER_ITEM_DIST(O_error_message,
                                       L_order_item_dist_tbl) = FALSE THEN
        --
        ROLLBACK TO INIT_REPL_REC;
        RETURN FALSE;
        --                    
      END IF;
      --
      L_order_item_dist_tbl.delete;
      --
      -- insert record into ma_stg_sizing_option_dist table
      --
      L_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
      --
      L_sizing_option_dist_obj := MA_STG_SIZING_OPTION_DIST_OBJ(seq_no               => L_seq_no,
                                                                master_order_no      => O_master_order_no,
                                                                order_no             => L_order_no,
                                                                option_id            => C_rec.option_id,
                                                                final_dest           => C_rec.final_dest,
                                                                exp_delivery_date    => L_handover_date,
                                                                qty_ordered          => L_qty_ordered,
                                                                sizing_applied       => 'Y',
                                                                size_profile         => C_rec.size_profile,
                                                                size_group           => C_rec.diff_2,
                                                                distributed_by       => 'Q',
                                                                supplier_reference   => C_rec.vpn,
                                                                create_id            => get_app_user,
                                                                create_datetime      => L_audit_date,
                                                                last_update_id       => get_app_user,
                                                                last_update_datetime => L_audit_date);
      --
      L_sizing_option_dist_tbl := MA_STG_SIZING_OPTION_DIST_TBL();
      L_sizing_option_dist_tbl.extend();
      L_sizing_option_dist_tbl(L_sizing_option_dist_tbl.count) := L_sizing_option_dist_obj;
      --
      IF INSERT_MA_SIZING_OPTION_DIST(O_error_message,
                                      L_sizing_option_dist_tbl) = FALSE THEN
        --
        ROLLBACK TO INIT_REPL_REC;
        RETURN FALSE;
        --                    
      END IF;
      --
      L_sizing_option_dist_tbl.delete;

    END IF;
    --
    -- insert record into ma_stg_sizing_sku table
    --
    L_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
    --
    L_sizing_sku_obj := MA_STG_SIZING_SKU_OBJ(seq_no               => L_seq_no,
                                              master_order_no      => O_master_order_no,
                                              order_no             => L_order_no,
                                              option_id            => C_rec.option_id,
                                              final_dest           => C_rec.final_dest,
                                              exp_delivery_date    => C_rec.need_date,
                                              sku                  => C_rec.sku_id,
                                              size_code            => C_rec.size_code,
                                              percentage           => L_percentage,
                                              ratio                => L_ratio,
                                              qty_ordered          => C_rec.qty_ordered,
                                              create_id            => get_app_user,
                                              create_datetime      => L_audit_date,
                                              last_update_id       => get_app_user,
                                              last_update_datetime => L_audit_date);
    --
    L_sizing_sku_tbl := MA_STG_SIZING_SKU_TBL();
    L_sizing_sku_tbl.extend();
    L_sizing_sku_tbl(L_sizing_sku_tbl.count) := L_sizing_sku_obj;
    --
    IF INSERT_MA_STG_SIZING_SKU(O_error_message,
                                L_sizing_sku_tbl) = FALSE THEN
      --
      ROLLBACK TO INIT_REPL_REC;
      RETURN FALSE;
      --                    
    END IF;
    --
    L_sizing_sku_tbl.delete;
    --
    L_option_id := C_rec.option_id;
    --
  END LOOP;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => L_order_rec_no,
                                              I_error_key         => 'CREATE_ORDER_REC_PLANNING',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END CREATE_ORDER_REC_REPLENISHMENT;
--------------------------------------------------------------------------------
FUNCTION CHECK_IF_ORDER_COMPLETE (O_error_message   OUT VARCHAR2, 
                                  I_master_order_no IN  MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN NUMBER IS
  --
  L_program           VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.CHECK_IF_ORDER_COMPLETE';  
  L_size_applied      NUMBER := NULL;
  L_size_not_applied  NUMBER := NULL;
  L_po                NUMBER := NULL;
  --
BEGIN
  --
  --  0 - OPTION SUPPLIER SCREEN
  --  1 - PURCHASE ORDER SCREEN
  --  2 - SIZING SCREEN
  --
  -- SIZING
  --
  SELECT COUNT(1) 
    INTO L_size_applied 
    FROM ma_stg_sizing_option_dist
   WHERE master_order_no = I_master_order_no
     AND sizing_applied  = 'Y';
  --    
  SELECT COUNT(1) 
    INTO L_size_not_applied 
    FROM MA_STG_SIZING_OPTION_DIST
   WHERE master_order_no = I_master_order_no;
  --
  IF (L_size_applied=L_size_not_applied) THEN
    --
      SELECT COUNT(1) 
      INTO L_po 
      FROM MA_STG_ORDER_DROPS
      WHERE master_order_no = I_master_order_no;
      --
      IF (L_po>0) THEN
        --
        RETURN 2;
        --
      END IF;
    --
  END IF;
  --   
  -- PURCHASE ORDER
  --
  SELECT COUNT(1) 
    INTO L_po 
    FROM MA_STG_ORDER_DROPS
   WHERE master_order_no = I_master_order_no;
  --
  IF (L_po>0) THEN
    --
    RETURN 1;
    --
  END IF;
  --
  RETURN 0;
  --
EXCEPTION
  --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_IS_GET_NEXT_ORDER_NBR',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN 0;
    --
  --
END CHECK_IF_ORDER_COMPLETE;
--------------------------------------------------------------------------------
FUNCTION LOAD_ORDER_TO_STG (O_error_message    OUT    VARCHAR2,
                            I_get_type         IN     VARCHAR2,
                            IO_master_order_no IN OUT MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN BOOLEAN IS
  --
  L_program                VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.LOAD_ORDER_TO_STG';  
  PROGRAM_ERROR            EXCEPTION;
  L_stg_order_tbl          MA_STG_ORDER_TBL;
  L_order_option_tbl       MA_STG_ORDER_OPTION_TBL;
  L_order_item_dist_tbl    MA_STG_ORDER_ITEM_DIST_TBL;
  L_sizing_option_dist_tbl MA_STG_SIZING_OPTION_DIST_TBL;
  L_sizing_sku_tbl         MA_STG_SIZING_SKU_TBL;
  L_order_drops_tbl        MA_STG_ORDER_DROPS_TBL;
  L_order_drops_detail_tbl MA_STG_ORDER_DROPS_DETAIL_TBL;
  L_audit_date             DATE := SYSDATE;
  L_master_order_no        MA_STG_ORDER.MASTER_ORDER_NO%TYPE   := IO_master_order_no;
  L_multi_po_ind           MA_STG_ORDER_DROP.MULTI_PO_IND%TYPE := 'N';
  L_drop_count             NUMBER := 0;
  --
  CURSOR C_get_stg_order IS
    SELECT NEW MA_STG_ORDER_OBJ(master_order_no, 
                                supplier, 
                                contract_no, 
                                comments, 
                                status, 
                                submitted_date, 
                                approved_by, 
                                approved_date, 
                                create_id, 
                                create_datetime, 
                                last_update_id, 
                                last_update_datetime)
      FROM (SELECT IO_master_order_no master_order_no,
                   supplier,
                   contract_no,
                   comments,
                   status,
                   submitted_date,
                   approved_by,
                   approved_date,
                   create_id,
                   create_datetime,
                   last_update_id,
                   last_update_datetime
              FROM (SELECT supplier,
                           contract_no,
                           comments,
                           DECODE(I_get_type, 'C', 'W', status) status,
                           DECODE(I_get_type, 'C', TO_DATE(NULL), submitted_date) submitted_date,
                           DECODE(I_get_type, 'C', NULL, approved_by) approved_by,
                           DECODE(I_get_type, 'C', TO_DATE(NULL), approved_date) approved_date,
                           DECODE(I_get_type, 'E', create_id, get_app_user) create_id,
                           DECODE(I_get_type, 'E', create_datetime, L_audit_date) create_datetime,
                           DECODE(I_get_type, 'E', last_update_id, get_app_user) last_update_id,
                           DECODE(I_get_type, 'E', last_update_datetime, L_audit_date) last_update_datetime
                      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER(L_master_order_no))
                   )
            );
  --
  CURSOR C_get_stg_order_option IS
    SELECT NEW MA_STG_ORDER_OPTION_OBJ(master_order_no, 
                                       option_id, 
                                       unit_cost, 
                                       qty_ordered, 
                                       seq_no, 
                                       factory, 
                                       manu_country_id, 
                                       size_group, 
                                       supplier_reference, 
                                       supplier_colour, 
                                       packing_method, 
                                       factory_risk_rating, 
                                       create_id, 
                                       create_datetime, 
                                       last_update_id, 
                                       last_update_datetime)
      FROM (SELECT IO_master_order_no master_order_no,
                   option_id, 
                   unit_cost, 
                   qty_ordered, 
                   seq_no, 
                   factory, 
                   manu_country_id, 
                   size_group, 
                   supplier_reference, 
                   supplier_colour, 
                   packing_method, 
                   factory_risk_rating, 
                   create_id, 
                   create_datetime, 
                   last_update_id, 
                   last_update_datetime
              FROM (SELECT master_order_no, 
                           option_id, 
                           unit_cost, 
                           qty_ordered, 
                           NVL(seq_no, rownum) seq_no, 
                           factory, 
                           manu_country_id, 
                           size_group, 
                           supplier_reference, 
                           supplier_colour, 
                           packing_method, 
                           factory_risk_rating, 
                           DECODE(I_get_type, 'E', create_id, get_app_user) create_id,
                           DECODE(I_get_type, 'E', create_datetime, L_audit_date) create_datetime,
                           DECODE(I_get_type, 'E', last_update_id, get_app_user) last_update_id,
                           DECODE(I_get_type, 'E', last_update_datetime, L_audit_date) last_update_datetime
                      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_OPTION(L_master_order_no))
                   )
            ); 
  --
  CURSOR C_get_stg_order_item_dist IS
    SELECT NEW MA_STG_ORDER_ITEM_DIST_OBJ(id_seq, 
                                          master_order_no, 
                                          po_type, 
                                          option_id, 
                                          first_dest, 
                                          final_dest, 
                                          handover_date, 
                                          qty_ordered, 
                                          unit_cost, 
                                          ship_method, 
                                          supplier_reference, 
                                          ship_method_final_dest, 
                                          ship_date, 
                                          first_dest_date, 
                                          not_before_date, 
                                          not_after_date, 
                                          final_dest_date, 
                                          freight_forward, 
                                          ship_port, 
                                          del_port, 
                                          week_no,
                                          ex_factory_date, 
                                          create_id, 
                                          create_datetime, 
                                          last_update_id, 
                                          last_update_datetime)
      FROM (SELECT id_seq,
                   IO_master_order_no master_order_no,
                   po_type, 
                   option_id, 
                   first_dest, 
                   final_dest, 
                   handover_date, 
                   qty_ordered, 
                   unit_cost, 
                   ship_method, 
                   supplier_reference, 
                   ship_method_final_dest, 
                   ship_date, 
                   first_dest_date, 
                   not_before_date, 
                   not_after_date, 
                   final_dest_date, 
                   freight_forward, 
                   ship_port, 
                   del_port,
                   week_no, 
                   ex_factory_date, 
                   create_id, 
                   create_datetime, 
                   last_update_id, 
                   last_update_datetime
              FROM (SELECT MA_ORDER_UTILS_SQL.GET_ITEM_DIST_ID_SEQ id_seq, 
                           tab.po_type, 
                           tab.option_id, 
                           tab.first_dest, 
                           tab.final_dest, 
                           tab.handover_date, 
                           tab.qty_ordered, 
                           tab.unit_cost, 
                           tab.ship_method, 
                           tab.supplier_reference, 
                           tab.ship_method_final_dest, 
                           tab.ship_date, 
                           tab.first_dest_date, 
                           tab.not_before_date, 
                           tab.not_after_date, 
                           tab.final_dest_date, 
                           tab.freight_forward, 
                           tab.ship_port, 
                           tab.del_port,
                           TO_NUMBER(TO_CHAR(tab.final_dest_date,'IW')) week_no, 
                           TO_DATE(MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS(I_master_order_no => L_master_order_no,
                                                                          I_order_no        => tab.order_no,
                                                                          I_cfa_type        => 'EX_FACTORY_DATE'),'DD-MM-YYYY') ex_factory_date, 
                           DECODE(I_get_type, 'E', o.create_id, get_app_user) create_id,
                           DECODE(I_get_type, 'E', o.create_datetime, L_audit_date) create_datetime,
                           DECODE(I_get_type, 'E', o.last_update_id, get_app_user) last_update_id,
                           DECODE(I_get_type, 'E', o.last_update_datetime, L_audit_date) last_update_datetime
                      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(L_master_order_no)) tab,
                           TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER(L_master_order_no)) o
                     WHERE tab.master_order_no = o.master_order_no
                  )        
            );
  --
   CURSOR C_get_drop_count IS
     SELECT COUNT(1)
       FROM ma_stg_order_drops
      WHERE master_order_no=IO_master_order_no;
  --
  CURSOR C_get_multi_option IS
    SELECT 'Y'
      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS(L_master_order_no))
     WHERE option_id IS NULL;
  --         
  CURSOR C_get_stg_sizing_option_dist IS
    SELECT NEW MA_STG_SIZING_OPTION_DIST_OBJ(seq_no, 
                                             master_order_no, 
                                             order_no, 
                                             option_id, 
                                             final_dest, 
                                             exp_delivery_date, 
                                             qty_ordered, 
                                             sizing_applied, 
                                             size_profile, 
                                             size_group, 
                                             distributed_by, 
                                             supplier_reference, 
                                             create_id, 
                                             create_datetime, 
                                             last_update_id, 
                                             last_update_datetime)
      FROM (SELECT tab.seq_no,
                   IO_master_order_no master_order_no,
                   (SELECT order_no
                      FROM ma_stg_order_drops_detail od
                     WHERE od.master_order_no   = IO_master_order_no
                       AND od.option_id         = tab.option_id
                       AND od.final_dest        = tab.final_dest
                       AND od.final_dest_date   = tab.exp_delivery_date
                       AND od.seq_no            = tab.seq_no
                   ) order_no,
                   tab.option_id, 
                   tab.final_dest, 
                   tab.exp_delivery_date, 
                   tab.qty_ordered, 
                   tab.sizing_applied, 
                   tab.size_profile, 
                   tab.size_group, 
                   tab.distributed_by, 
                   tab.supplier_reference, 
                   tab.create_id, 
                   tab.create_datetime, 
                   tab.last_update_id, 
                   tab.last_update_datetime
              FROM (SELECT (SELECT dds.seq_no
                              FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(L_master_order_no)) d,
                                   ma_stg_order_drops_detail dds
                             WHERE d.option_id       = od.option_id
                                   AND d.final_dest         = od.final_dest
                                  -- AND d.final_dest_date    = od.exp_delivery_date
                                  -- AND d.order_no           = od.order_no
                                   AND dds.master_order_no  = IO_master_order_no
                                   AND dds.option_id        = d.option_id
                                   AND dds.first_dest       = d.first_dest
                                   AND dds.final_dest       = d.final_dest
                                   AND dds.handover_date    = d.handover_date
                           ) seq_no,
                           order_no,
                           option_id, 
                           final_dest, 
                           exp_delivery_date, 
                           qty_ordered, 
                           sizing_applied, 
                           size_profile, 
                           size_group, 
                           distributed_by, 
                           supplier_reference, 
                           DECODE(I_get_type, 'E', create_id, get_app_user) create_id,
                           DECODE(I_get_type, 'E', create_datetime, L_audit_date) create_datetime,
                           DECODE(I_get_type, 'E', last_update_id, get_app_user) last_update_id,
                           DECODE(I_get_type, 'E', last_update_datetime, L_audit_date) last_update_datetime
                      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_OPTION(L_master_order_no)) od
                   )tab
            );
  --
  CURSOR C_get_stg_drops IS
    SELECT NEW MA_STG_ORDER_DROPS_OBJ(master_order_no, 
                                      order_no, 
                                      po_type, 
                                      option_id, 
                                      first_dest, 
                                      final_dest, 
                                      handover_date, 
                                      qty_ordered, 
                                      unit_cost, 
                                      ship_method, 
                                      ship_date, 
                                      first_dest_date, 
                                      not_before_date, 
                                      not_after_date, 
                                      final_dest_date, 
                                      freight_forward, 
                                      ship_port, 
                                      del_port, 
                                      supplier_reference, 
                                      ship_method_final_dest, 
                                      rev_no, 
                                      create_id, 
                                      create_datetime, 
                                      last_update_id, 
                                      last_update_datetime)
      FROM (SELECT d.master_order_no, 
                   d.order_no, 
                   d.po_type, 
                   d.option_id, 
                   d.first_dest, 
                   d.final_dest, 
                   d.handover_date, 
                   d.qty_ordered, 
                   d.unit_cost, 
                   d.ship_method, 
                   d.ship_date, 
                   d.first_dest_date, 
                   d.not_before_date, 
                   d.not_after_date, 
                   d.final_dest_date, 
                   d.freight_forward, 
                   d.ship_port, 
                   d.del_port, 
                   d.supplier_reference, 
                   d.ship_method_final_dest, 
                   d.rev_no, 
                   o.create_id,
                   o.create_datetime,
                   o.last_update_id,
                   o.last_update_datetime
              FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS(L_master_order_no)) d,
                   TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER(L_master_order_no)) o
             WHERE d.master_order_no = o.master_order_no);
  --  
  CURSOR C_get_stg_drops_detail IS
    SELECT NEW MA_STG_ORDER_DROPS_DETAIL_OBJ(master_order_no, 
                                             order_no, 
                                             po_type, 
                                             option_id, 
                                             first_dest, 
                                             final_dest, 
                                             handover_date, 
                                             qty_ordered, 
                                             unit_cost, 
                                             ship_method, 
                                             ship_date, 
                                             first_dest_date, 
                                             not_before_date, 
                                             not_after_date, 
                                             final_dest_date, 
                                             freight_forward, 
                                             ship_port, 
                                             del_port, 
                                             supplier_reference, 
                                             ship_method_final_dest, 
                                             seq_no, 
                                             create_id, 
                                             create_datetime, 
                                             last_update_id, 
                                             last_update_datetime)
      FROM (SELECT d.master_order_no, 
                   d.order_no, 
                   d.po_type, 
                   d.option_id, 
                   d.first_dest, 
                   d.final_dest, 
                   d.handover_date, 
                   d.qty_ordered, 
                   d.unit_cost, 
                   d.ship_method, 
                   d.ship_date, 
                   d.first_dest_date, 
                   d.not_before_date, 
                   d.not_after_date, 
                   d.final_dest_date, 
                   d.freight_forward, 
                   d.ship_port, 
                   d.del_port, 
                   d.supplier_reference, 
                   d.ship_method_final_dest, 
                   id.id_seq seq_no, 
                   o.create_id,
                   o.create_datetime,
                   o.last_update_id,
                   o.last_update_datetime
              FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(L_master_order_no)) d,
                   TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER(L_master_order_no)) o,
                   ma_stg_order_item_dist id
             WHERE d.master_order_no  = o.master_order_no
               AND id.master_order_no = d.master_order_no
               AND id.option_id       = d.option_id
               AND id.handover_date   = d.handover_date
               AND id.first_dest      = d.first_dest
               AND id.final_dest      = d.final_dest
               AND id.po_type         = d.po_type
               AND id.ship_method     = d.ship_method
               AND id.freight_forward = d.freight_forward
               AND id.ship_port       = d.ship_port);
  --  
  CURSOR C_get_stg_sizing_sku IS
    SELECT NEW MA_STG_SIZING_SKU_OBJ(seq_no,
                                     master_order_no, 
                                     order_no, 
                                     option_id, 
                                     final_dest, 
                                     exp_delivery_date, 
                                     sku, 
                                     size_code, 
                                     percentage, 
                                     ratio, 
                                     qty_ordered, 
                                     create_id, 
                                     create_datetime, 
                                     last_update_id, 
                                     last_update_datetime)
      FROM (SELECT tab.seq_no,
                   IO_master_order_no master_order_no,
                   (SELECT order_no
                      FROM ma_stg_sizing_option_dist od
                     WHERE od.master_order_no   = IO_master_order_no
                       AND od.option_id         = tab.option_id
                       AND od.final_dest        = tab.final_dest
                       AND od.exp_delivery_date = tab.exp_delivery_date
                       AND od.seq_no            = tab.seq_no
                   ) order_no, 
                   tab.option_id, 
                   tab.final_dest, 
                   tab.exp_delivery_date, 
                   tab.sku, 
                   tab.size_code, 
                   tab.percentage, 
                   tab.ratio, 
                   tab.qty_ordered, 
                   tab.create_id, 
                   tab.create_datetime, 
                   tab.last_update_id, 
                   tab.last_update_datetime
              FROM (SELECT (SELECT dds.seq_no
                         FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(L_master_order_no)) d,
                              ma_stg_order_drops_detail dds
                         WHERE d.option_id       = ss.option_id
                               AND d.final_dest         = ss.final_dest
                               AND d.final_dest_date    = ss.exp_delivery_date
                               AND d.order_no           = ss.order_no
                               AND dds.master_order_no  = IO_master_order_no
                               AND dds.option_id        = d.option_id
                               AND dds.first_dest       = d.first_dest
                               AND dds.final_dest       = d.final_dest
                               AND dds.handover_date    = d.handover_date
                           ) seq_no,
                           option_id, 
                           final_dest, 
                           exp_delivery_date, 
                           sku, 
                           size_code, 
                           percentage, 
                           ratio, 
                           qty_ordered, 
                           DECODE(I_get_type, 'E', create_id, get_app_user) create_id,
                           DECODE(I_get_type, 'E', create_datetime, L_audit_date) create_datetime,
                           DECODE(I_get_type, 'E', last_update_id, get_app_user) last_update_id,
                           DECODE(I_get_type, 'E', last_update_datetime, L_audit_date) last_update_datetime
                      FROM TABLE( MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU(L_master_order_no)) ss
                   )tab
            );
  --
BEGIN
  --
  -- Delete possible orders in pub info
  --
  DELETE ma_order_pub_info
    WHERE order_no IN (SELECT order_no
                         FROM ma_stg_order_drops
                        WHERE master_order_no = IO_master_order_no
                       );
  -- 
  IF I_get_type <> 'E' THEN
    --
    IO_master_order_no := MA_ORDER_UTILS_SQL.GET_ORDER_SEQ;
    --
  ELSE
    --
    IF delete_stg_order_tables(O_error_message,
                               L_master_order_no) = FALSE THEN
    --
    RETURN FALSE;
    --                    
    END IF;
    --
    IO_master_order_no := L_master_order_no;
    --
  END IF;
  --
  -- insert record into ma_stg_order table
  --
  OPEN C_get_stg_order;
  FETCH C_get_stg_order BULK COLLECT INTO L_stg_order_tbl;
  CLOSE C_get_stg_order;
  --
  IF INSERT_MA_STG_ORDER(O_error_message,
                         L_stg_order_tbl) = FALSE THEN
    --
    RETURN FALSE;
    --                    
  END IF;
  --
  -- insert record into ma_stg_order_option table
  --
  OPEN C_get_stg_order_option;
  FETCH C_get_stg_order_option BULK COLLECT INTO L_order_option_tbl;
  CLOSE C_get_stg_order_option;
  --
  IF INSERT_MA_STG_ORDER_OPTION(O_error_message,
                                L_order_option_tbl) = FALSE THEN
    --
    RETURN FALSE;
    --                    
  END IF;
  --
  -- insert record into ma_stg_order_item_dist table
  --
  OPEN C_get_stg_order_item_dist;
  FETCH C_get_stg_order_item_dist BULK COLLECT INTO L_order_item_dist_tbl;
  CLOSE C_get_stg_order_item_dist;
  --
  IF INSERT_MA_STG_ORDER_ITEM_DIST(O_error_message,
                                   L_order_item_dist_tbl) = FALSE THEN
    --
    RETURN FALSE;
    --                    
  END IF;
  --
  -- insert record into ma_stg_order_drop table
  --
  OPEN C_get_multi_option;
  FETCH C_get_multi_option INTO L_multi_po_ind;
  CLOSE C_get_multi_option;
  --
  INSERT INTO ma_stg_order_drop
          (master_order_no, 
           multi_po_ind, 
           create_id, 
           create_datetime, 
           last_update_id, 
           last_update_datetime)
    SELECT IO_master_order_no,
           L_multi_po_ind,
           DECODE(I_get_type, 'E', create_id, get_app_user) create_id,
           DECODE(I_get_type, 'E', create_datetime, L_audit_date) create_datetime,
           DECODE(I_get_type, 'E', last_update_id, get_app_user) last_update_id,
           DECODE(I_get_type, 'E', last_update_datetime, L_audit_date) last_update_datetime
      FROM ma_stg_order
     WHERE master_order_no = IO_master_order_no;
  --
  -- Insert records into ma_stg_order_drops and ma_stg_order_drops_detail
  --
  IF I_get_type = 'C' THEN
    --
    IF MA_ORDER_UTILS_SQL.CREATE_DROP_DIST(O_error_message   => O_error_message,
                                           I_master_order_no => IO_master_order_no,
                                           I_option_drop     => L_multi_po_ind,
                                           I_option_id       => NULL,
                                           I_new_qty         => 0) = FALSE THEN
      RETURN FALSE;
    END IF;
    --
  ELSE
    --
    -- Insert into ma_stg_drops
    --
    OPEN C_get_stg_drops;
    FETCH C_get_stg_drops BULK COLLECT INTO L_order_drops_tbl;
    CLOSE C_get_stg_drops;
    --
    IF INSERT_MA_STG_ORDER_DROPS(O_error_message,
                                 L_order_drops_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
    -- Insert into ma_stg_drops_detail
    --
    OPEN C_get_stg_drops_detail;
    FETCH C_get_stg_drops_detail BULK COLLECT INTO L_order_drops_detail_tbl;
    CLOSE C_get_stg_drops_detail;
    --
    IF INSERT_MA_STG_ORDER_DROPS_DTL(O_error_message,
                                     L_order_drops_detail_tbl) = FALSE THEN
      --
      RETURN FALSE;
      --                    
    END IF;
    --
  END IF;
  --
  OPEN C_get_drop_count;
  FETCH C_get_drop_count INTO L_drop_count;
  CLOSE C_get_drop_count;
  --
  IF L_drop_count>0 THEN
      --
      -- insert record into ma_stg_sizing_option_dist table
      --
      OPEN C_get_stg_sizing_option_dist;
      FETCH C_get_stg_sizing_option_dist BULK COLLECT INTO L_sizing_option_dist_tbl;
      CLOSE C_get_stg_sizing_option_dist;
      --
      IF INSERT_MA_SIZING_OPTION_DIST(O_error_message,
                                      L_sizing_option_dist_tbl) = FALSE THEN
        --
        RETURN FALSE;
        --                    
      END IF;
      --
      -- insert record into ma_stg_sizing_sku table
      --
      OPEN C_get_stg_sizing_sku;
      FETCH C_get_stg_sizing_sku BULK COLLECT INTO L_sizing_sku_tbl;
      CLOSE C_get_stg_sizing_sku;
      --
      IF INSERT_MA_STG_SIZING_SKU(O_error_message,
                                  L_sizing_sku_tbl) = FALSE THEN
        --
        RETURN FALSE;
        --                    
      END IF;
  --
  END IF;
  --  
  IF I_get_type <> 'E' THEN
  --
  -- Delete final destination date from ma_stg_order_item_dist and ma_stg_order_drops_detail
  --
    UPDATE MA_STG_ORDER_ITEM_DIST
      SET final_dest_date = NULL,
          handover_date   = NULL,
          ship_date       = NULL, 
          first_dest_date = NULL, 
          not_before_date = NULL, 
          not_after_date  = NULL,
          ex_factory_date = NULL,
          week_no         = NULL
      WHERE master_order_no = IO_master_order_no; 
  --
    DELETE FROM MA_STG_ORDER_DROPS_DETAIL 
      WHERE master_order_no = IO_master_order_no; 
  --
    DELETE FROM MA_STG_ORDER_DROPS 
      WHERE master_order_no = IO_master_order_no; 
  --
  END IF;
  --
  -- Insert costs
  --
  IF I_get_type = 'E' THEN
    --
    -- Insert into ma_stg_cost_option_detail
    --
    INSERT INTO ma_stg_cost_option_detail
                 (order_level, 
                  master_order_no, 
                  option_id, 
                  supplier_reference, 
                  order_no, 
                  qty_ordered, 
                  first_dest, 
                  final_dest, 
                  supplier_currency, 
                  unit_cost, 
                  total_unit_cost, 
                  total_discount_cost, 
                  fc_currency, 
                  total_expense, 
                  total_upcharge, 
                  total_duty, 
                  total_landed_cost, 
                  retail_price, 
                  buy_value, 
                  plan_buy_margin, 
                  exp_buy_margin)
          WITH tab_data AS
                (SELECT  /*+ materialize */ order_level, 
                        master_order_no, 
                        option_id, 
                        supplier_reference, 
                        order_no, 
                        qty_ordered, 
                        first_dest, 
                        final_dest, 
                        supplier_currency, 
                        unit_cost, 
                        total_unit_cost, 
                        total_discount_cost, 
                        fc_currency, 
                        total_expense, 
                        total_upcharge, 
                        total_duty, 
                        total_landed_cost, 
                        retail_price, 
                        buy_value, 
                        plan_buy_margin, 
                        exp_buy_margin
                  FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_COST_OPTION_DETAIL(IO_master_order_no)))
            SELECT * FROM tab_data;
    --
    -- Insert into ma_stg_cost_drop_detail
    --
    INSERT INTO ma_stg_cost_drop_detail
                 (order_level, 
                  master_order_no, 
                  order_no, 
                  fc, 
                  currency_code, 
                  total_unit_cost, 
                  total_discount_cost, 
                  total_expense, 
                  total_duty, 
                  total_landed_cost, 
                  total_retail_value, 
                  exp_buy_margin)
          WITH tab_data AS
              (SELECT /*+ materialize */ order_level, 
                      master_order_no, 
                      order_no, 
                      fc, 
                      currency_code, 
                      total_unit_cost, 
                      total_discount_cost, 
                      total_expense, 
                      total_duty, 
                      total_landed_cost, 
                      total_retail_value, 
                      exp_buy_margin
                FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_COST_DROP_DETAIL(IO_master_order_no)))
          SELECT * FROM tab_data;
    --
    -- Insert into ma_stg_cost_expense_detail
    --
    INSERT INTO ma_stg_cost_expense_detail
                 (expense_id, 
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
          WITH tab_data AS
              (SELECT /*+ materialize */ expense_id, 
                      order_level, 
                      master_order_no, 
                      order_no, 
                      option_id, 
                      qty_ordered, 
                      fc, 
                      fc_desc, 
                      supplier_size, 
                      sku, 
                      cost_component, 
                      currency_code, 
                      rate, 
                      total_value
                FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_COST_EXPENSE_DETAIL(IO_master_order_no)))
           SELECT ma_stg_cost_expense_detail_seq.nextval,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value
             FROM tab_data;
    --
    -- Insert into ma_stg_cost_up_charge_detail
    --
    INSERT INTO ma_stg_cost_up_charge_detail
                 (order_level, 
                  master_order_no, 
                  option_id, 
                  order_no, 
                  qty_ordered, 
                  first_dest, 
                  final_dest, 
                  supplier_size, 
                  sku, 
                  sku_desc, 
                  cost_component, 
                  currency_code, 
                  rate, 
                  total_value)
          WITH tab_data AS
              (SELECT /*+ materialize */ order_level, 
                      master_order_no, 
                      option_id, 
                      order_no, 
                      qty_ordered, 
                      first_dest, 
                      final_dest, 
                      supplier_size, 
                      sku, 
                      sku_desc, 
                      cost_component, 
                      currency_code, 
                      rate, 
                      total_value
                FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_COST_UP_CHARGE_DETAIL(IO_master_order_no)))
           SELECT * FROM tab_data;
    --
    -- Insert into ma_stg_cost_up_charge_detail
    --
    INSERT INTO ma_stg_cost_duty_detail
                 (duty_id, 
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  commodity_code, 
                  rate, 
                  total_value)
          WITH tab_data AS
              (SELECT /*+ materialize */ duty_id, 
                      order_level, 
                      master_order_no, 
                      order_no, 
                      option_id, 
                      qty_ordered, 
                      fc, 
                      fc_desc, 
                      supplier_size, 
                      sku, 
                      cost_component, 
                      currency_code, 
                      commodity_code, 
                      rate, 
                      total_value
                FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_COST_DUTY_DETAIL(IO_master_order_no)))
           SELECT ma_stg_cost_duty_detail_seq.nextval,
                  order_level, 
                  master_order_no, 
                  order_no, 
                  option_id, 
                  qty_ordered, 
                  fc, 
                  fc_desc, 
                  supplier_size, 
                  sku, 
                  cost_component, 
                  currency_code, 
                  commodity_code, 
                  rate, 
                  total_value
             FROM tab_data;
    --
  END IF;
  --
  -- Insert orders into pubinfo 
  --
  --
  -- Delete possible orders in pub info
  --
  DELETE ma_order_pub_info
    WHERE order_no IN (SELECT order_no
                         FROM ma_stg_order_drops
                        WHERE master_order_no = IO_master_order_no
                       );
  --
  DELETE ma_order_mfqueue
   WHERE master_order_no = IO_master_order_no;
  --
  INSERT INTO ma_order_pub_info
                     (order_no,
                      initial_approval_ind,
                      thread_no,
                      published,
                      order_type)
               SELECT DISTINCT
                      order_no,
                      NULL,
                      NULL,
                      DECODE(I_get_type, 'E', 'Y', 'N'),
                      po_type
                 FROM ma_stg_order_drops
                WHERE master_order_no = IO_master_order_no;
  --
  RETURN TRUE;
  --
EXCEPTION
  --
  WHEN PROGRAM_ERROR THEN
    --
    ROLLBACK;
    RETURN FALSE;
    --
  WHEN OTHERS THEN
    --
    O_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_aux_1             => IO_master_order_no,
                                              I_error_key         => 'LOAD_ORDER_TO_STG',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    RETURN FALSE;
    --
END LOAD_ORDER_TO_STG;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_SIZING_SKU(I_master_order_no IN MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingSkuObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU';
  L_error_message VARCHAR2(2000);
  --
  L_sizing_sku OrderSizingSkuObjTbl;
  --
  CURSOR C_get_sizing_sku(v_master_order_no MA_STG_SIZING_SKU.MASTER_ORDER_NO%TYPE) IS
    WITH ord AS
      (SELECT *
         FROM (SELECT master_po_no master_order_no,
                      order_no,
                      supplier,
                      item_parent option_id,
                      'D' po_type,
                      not_after_date,
                      location loc,
                      SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0)) qty_ordered
                 FROM (SELECT oh.master_po_no,
                              oh.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ol.location,
                              NULL qty_allocated,
                              SUM(ol.qty_ordered) qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              ordloc ol,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.status       = 'A'
                          and ol.order_no     = oh.order_no
                          and ol.item         = im.item
                       GROUP BY oh.master_po_no,
                                oh.order_no,
                                oh.supplier,
                                oh.po_type,
                                trunc(oh.not_after_date),
                                ol.location,
                                im.item_parent
                       UNION ALL
                       SELECT oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ah.wh,
                              sum(ad.qty_allocated),
                              NULL qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              alloc_header ah,
                              alloc_detail ad,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.order_no     = ah.order_no
                          AND ad.alloc_no     = ah.alloc_no
                          AND oh.status       = 'A'
                          AND im.item = ah.item
                     GROUP BY oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date),
                              ah.wh,
                              im.item_parent
                    )
                HAVING (SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0))) > 0
                GROUP BY master_po_no,
                         order_no,
                         supplier,
                         item_parent,
                         not_after_date,
                         LOCATION
              )
              UNION ALL
              (SELECT oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date) not_after_date,
                      ad.to_loc,
                      sum(ad.qty_allocated)
                 FROM ordhead oh,
                      alloc_header ah,
                      alloc_detail ad,
                      ma_v_item_master im
                WHERE oh.master_po_no = I_master_order_no
                  AND oh.order_no     = ah.order_no
                  AND ad.alloc_no     = ah.alloc_no
                  AND oh.status       = 'A'
                  AND im.item = ah.item
             GROUP BY oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date),
                      ad.to_loc)
      )
    SELECT OrderSizingSkuObj(master_order_no,
                             order_no,
                             option_id,
                             final_dest,
                             exp_delivery_date,
                             sku,
                             size_code,
                             percentage,
                             ratio,
                             qty_ordered,
                             create_id,
                             create_datetime,
                             last_update_id,
                             last_update_datetime)
      FROM (SELECT s.master_order_no,
                   s.order_no,
                   s.option_id,
                   s.final_dest,
                   s.exp_delivery_date,
                   s.sku,
                   s.size_code,
                   s.percentage,
                   s.ratio,
                   s.qty_ordered,
                   s.create_id,
                   s.create_datetime,
                   s.last_update_id,
                   s.last_update_datetime
              FROM ma_stg_sizing_sku s,
                   ma_stg_order o
             WHERE o.master_order_no = I_master_order_no
               AND s.master_order_no = o.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT master_order_no,
                   order_no,
                   option_id,
                   final_dest,
                   exp_delivery_date,
                   sku,
                   size_code,
                   percentage,
                   ratio,
                   qty_ordered,
                   create_id,
                   create_datetime,
                   last_update_id,
                   last_update_datetime
              FROM (SELECT ord.master_order_no,
                           ord.order_no,
                           ord.option_id,
                           ord.loc final_dest,
                           ord.not_after_date exp_delivery_date,
                           im.item sku,
                           im.diff_2 size_code,
                           NULL percentage,
                           NULL ratio,
                           CASE
                             WHEN ord.po_type = 'D' THEN
                               ol.qty_ordered - NVL((SELECT ad.qty_allocated
                                                       FROM alloc_header ah,
                                                            alloc_detail ad
                                                      WHERE ah.alloc_no = ad.alloc_no
                                                        AND ah.order_no = ord.order_no
                                                        AND ah.item     = im.item
                                                     ),0)
                             ELSE
                               (SELECT ad.qty_allocated
                                  FROM alloc_header ah,
                                       alloc_detail ad
                                 WHERE ah.alloc_no = ad.alloc_no
                                   AND ah.order_no = ord.order_no
                                   AND ah.item     = im.item
                                   AND ad.to_loc   = ord.loc
                               )
                          END qty_ordered,
                           oh.create_id,
                           oh.create_datetime,
                           oh.last_update_id,
                           oh.last_update_datetime
                      FROM ord                  ord,
                           ordhead              oh,
                           ordloc               ol,
                           ma_v_item_master     im
                     WHERE oh.order_no   = ord.order_no
                       AND ol.order_no   = ord.order_no
                       AND ol.item       = im.item
                       AND ord.option_id = im.item_parent
                   )
             WHERE qty_ordered > 0
           );
  --
BEGIN
  --
  open C_get_sizing_sku(I_master_order_no);
  --
  loop
    --
    fetch C_get_sizing_sku bulk collect into L_sizing_sku limit 100;
    exit when L_sizing_sku.count = 0;
    --
    for i in 1..L_sizing_sku.count loop
      --
      pipe row(OrderSizingSkuObj   (L_sizing_sku(i).master_order_no,
                                    L_sizing_sku(i).order_no,
                                    L_sizing_sku(i).option_id,
                                    L_sizing_sku(i).final_dest,
                                    L_sizing_sku(i).exp_delivery_date,
                                    L_sizing_sku(i).sku,
                                    L_sizing_sku(i).size_code,
                                    L_sizing_sku(i).percentage,
                                    L_sizing_sku(i).ratio,
                                    L_sizing_sku(i).qty_ordered,
                                    L_sizing_sku(i).create_id,
                                    L_sizing_sku(i).create_datetime,
                                    L_sizing_sku(i).last_update_id,
                                    L_sizing_sku(i).last_update_datetime));
        --
    end loop;
    --
  end loop;
  --
  close C_get_sizing_sku;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_SIZING_SKU',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_SIZING_SKU;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_OPTION(I_master_order_no IN MA_STG_ORDER_OPTION.MASTER_ORDER_NO%TYPE)
RETURN OrderOptionInfoObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ITEMS_UTILS_SQL.VIEWMODE_ORDER_OPTION';
  L_error_message VARCHAR2(2000);
  --
  L_order_option OrderOptionInfoObjTbl;
  --
  CURSOR C_get_order_options IS
    SELECT OrderOptionInfoObj(t.master_order_no,
                              t.option_id,
                              i.item_desc,
                              (i.dept||' / '||i.class||' / '||i.subclass),
                              (v.business_model||' / '||v.buying_group||' / '||v.buying_subgroup),
                              i.diff_1,
                              t.unit_cost,
                              t.qty_ordered,
                              t.seq_no,
                              t.factory,
                              t.manu_country_id,
                              t.size_group,
                              t.supplier_reference,
                              t.supplier_colour,
                              t.packing_method,
                              t.factory_risk_rating,
                              t.create_id,
                              t.create_datetime,
                              t.last_update_id,
                              t.last_update_datetime)
      FROM (SELECT t.master_order_no,
                   t.option_id,
                   t.unit_cost,
                   t.qty_ordered,
                   t.seq_no,
                   t.factory,
                   t.manu_country_id,
                   t.size_group,
                   t.supplier_reference,
                   t.supplier_colour,
                   t.packing_method,
                   t.factory_risk_rating,
                   t.create_id,
                   t.create_datetime,
                   t.last_update_id,
                   t.last_update_datetime
              FROM ma_stg_order_option t,
                   ma_stg_order o
             WHERE o.master_order_no = I_master_order_no
               AND t.master_order_no = o.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT master_po_no           master_order_no,
                   im.item_parent         option_id,
                   ol.unit_cost,
                   SUM(ol.qty_ordered)    qty_ordered,
                   NULL                   seq_no,
                   oh.factory,
                   sk.origin_country_id   manu_country_id,
                   (SELECT diff_2
                      FROM ma_v_item_master
                     WHERE item = im.item_parent
                   ) size_group,
                   os.vpn                 supplier_reference,
                   os.supp_diff_1         supplier_colour,
                   os.packing_method      packing_method,
                   MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS(I_master_order_no => I_master_order_no,
                                                          I_order_no        => oh.order_no,
                                                          I_cfa_type        => 'FACTORY_RISK_RATING') factory_risk_rating,
                   oh.create_id,
                   trunc(oh.create_datetime),
                   oh.last_update_id,
                   trunc(oh.last_update_datetime)
              FROM (SELECT *
                      FROM ordhead oh
                     WHERE oh.master_po_no = I_master_order_no
                       AND status = 'A'
                   ) oh,
                   ordloc                ol,
                   ordsku                sk,
                   ma_v_item_master      im,
                   (select distinct
                        supplier,
                        item,
                        vpn,
                        supp_diff_1,
                        packing_method
                      from ma_v_option_supplier)  os,
                   ma_v_supplier_factory sf
             WHERE ol.order_no = oh.order_no
               AND ol.item     = im.item
               AND os.item     = im.item_parent
               AND os.supplier = oh.supplier
               AND sf.supplier = oh.supplier
               AND sf.factory  = oh.factory
               AND sk.order_no = oh.order_no
               AND sk.item     = ol.item
            GROUP BY master_po_no,
                     im.item_parent,
                     ol.unit_cost,
                     oh.factory,
                     sk.origin_country_id,
                     os.vpn,
                     os.supp_diff_1,
                     os.packing_method,
                     MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS(I_master_order_no => I_master_order_no,
                                                            I_order_no        => oh.order_no,
                                                            I_cfa_type        => 'FACTORY_RISK_RATING'),
                     oh.create_id,
                     trunc(oh.create_datetime),
                     oh.last_update_id,
                     trunc(oh.last_update_datetime)
           )t,
           ma_v_item_master i,
           ma_v_buyerarchy  v
     where i.item = t.option_id
       AND v.item = t.option_id;
  --
BEGIN
  --
  open C_get_order_options;
  --
  loop
    --
    fetch C_get_order_options bulk collect into L_order_option limit 100;
    exit when L_order_option.count = 0;
    --
    for i in 1..L_order_option.count loop
      --
      pipe row(OrderOptionInfoObj(L_order_option(i).master_order_no,
                                  L_order_option(i).option_id,
                                  L_order_option(i).item_desc,
                                  L_order_option(i).product_hierarchy,
                                  L_order_option(i).buyrarchy,
                                  L_order_option(i).asos_colour,
                                  L_order_option(i).unit_cost,
                                  L_order_option(i).qty_ordered,
                                  L_order_option(i).seq_no,
                                  L_order_option(i).factory,
                                  L_order_option(i).manu_country_id,
                                  L_order_option(i).size_group,
                                  L_order_option(i).supplier_reference,
                                  L_order_option(i).supplier_colour,
                                  L_order_option(i).packing_method,
                                  L_order_option(i).factory_risk_rating,
                                  L_order_option(i).create_id,
                                  L_order_option(i).create_datetime,
                                  L_order_option(i).last_update_id,
                                  L_order_option(i).last_update_datetime));
      --
    end loop;
    --
  end loop;
  --
  close C_get_order_options;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_ORDER_OPTION',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_ORDER_OPTION;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER(I_master_order_no IN MA_STG_ORDER.MASTER_ORDER_NO%TYPE)
RETURN OrderInfoObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ITEMS_UTILS_SQL.VIEWMODE_ORDER';
  L_error_message VARCHAR2(2000);
  --
  L_order OrderInfoObjTbl;
  --
  CURSOR C_get_order IS
    SELECT OrderInfoObj(t.master_order_no,
                        t.supplier,
                        t.contract_no,
                        t.comments,
                        t.status,
                        t.approved_by,
                        t.approved_date,
                        t.submitted_date,
                        t.create_id,
                        t.create_datetime,
                        t.last_update_id,
                        t.last_update_datetime,
                        sup.sup_name,
                        sup.currency_code,
                        (select term_desc
                           from freight_terms_tl
                          where freight_terms = sup.freight_terms
                            and lang = 1),
                        (select terms_desc
                           from terms_head_tl
                          where terms = sup.terms
                            and lang  = 1))
      FROM (SELECT t.master_order_no,
                   t.supplier,
                   t.contract_no,
                   t.comments,
                   t.status,
                   t.approved_by,
                   t.approved_date,
                   t.submitted_date,
                   t.create_id,
                   t.create_datetime,
                   t.last_update_id,
                   t.last_update_datetime
              FROM ma_stg_order t
             WHERE t.status IN ('S','W')
               AND t.master_order_no = I_master_order_no
            UNION ALL
            SELECT t.master_po_no,
                   t.supplier,
                   t.contract_no,
                   t.comment_desc,
                   t.status,
                   t.orig_approval_id,
                   t.orig_approval_date,
                   TO_DATE(MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS(I_master_order_no => I_master_order_no,
                                                                  I_order_no        => t.order_no,
                                                                  I_cfa_type        => 'SUBMITTED_DATE'),'DD-MM-YYYY') submitted_date,
                   t.create_id,
                   t.create_datetime,
                   t.last_update_id,
                   t.last_update_datetime
              FROM ordhead t
             WHERE t.status = 'A'
               AND t.master_po_no = I_master_order_no
           ) t,
           ma_v_sups sup
     WHERE t.supplier = sup.supplier
       AND rownum < 2;
  --
BEGIN
  --
  open C_get_order;
  --
  loop
    --
    fetch C_get_order bulk collect into L_order limit 100;
    exit when L_order.count = 0;
    --
    for i in 1..L_order.count loop
      --
      pipe row(OrderInfoObj(L_order(i).master_order_no,
                            L_order(i).supplier,
                            L_order(i).contract_no,
                            L_order(i).comments,
                            L_order(i).status,
                            L_order(i).approved_by,
                            L_order(i).approved_date,
                            L_order(i).submitted_date,
                            L_order(i).create_id,
                            L_order(i).create_datetime,
                            L_order(i).last_update_id,
                            L_order(i).last_update_datetime,
                            L_order(i).sup_name,
                            L_order(i).currency_code,
                            L_order(i).freight_terms_desc,
                            L_order(i).terms_desc));
      --
    end loop;
    --
  end loop;
  --
  close C_get_order;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_ORDER',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_ORDER;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_DROPS(I_master_order_no IN MA_STG_ORDER_DROPS.MASTER_ORDER_NO%TYPE)
RETURN OrderDropsObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS';
  L_error_message VARCHAR2(2000);
  --
  L_OrderDrops OrderDropsObjTbl;
  --
  CURSOR C_get_order_drops IS
    WITH ord AS
      (SELECT *
         FROM (SELECT master_po_no master_order_no,
                      order_no,
                      supplier,
                      item_parent option_id,
                      'D' po_type,
                      not_after_date,
                      location loc,
                      SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0)) qty_ordered
                 FROM (SELECT oh.master_po_no,
                              oh.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ol.location,
                              NULL qty_allocated,
                              SUM(ol.qty_ordered) qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              ordloc ol,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.status       = 'A'
                          and ol.order_no     = oh.order_no
                          and ol.item         = im.item
                       GROUP BY oh.master_po_no,
                                oh.order_no,
                                oh.supplier,
                                oh.po_type,
                                trunc(oh.not_after_date),
                                ol.location,
                                im.item_parent
                       UNION ALL
                       SELECT oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ah.wh,
                              sum(ad.qty_allocated),
                              NULL qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              alloc_header ah,
                              alloc_detail ad,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.order_no     = ah.order_no
                          AND ad.alloc_no     = ah.alloc_no
                          AND oh.status       = 'A'
                          AND im.item = ah.item
                     GROUP BY oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date),
                              ah.wh,
                              im.item_parent
                    )
                HAVING (SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0))) > 0
                GROUP BY master_po_no,
                         order_no,
                         supplier,
                         item_parent,
                         not_after_date,
                         LOCATION
              )
              UNION ALL
              (SELECT oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date) not_after_date,
                      ad.to_loc,
                      sum(ad.qty_allocated)
                 FROM ordhead oh,
                      alloc_header ah,
                      alloc_detail ad,
                      ma_v_item_master im
                WHERE oh.master_po_no = I_master_order_no
                  AND oh.order_no     = ah.order_no
                  AND ad.alloc_no     = ah.alloc_no
                  AND oh.status       = 'A'
                  AND im.item = ah.item
             GROUP BY oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date),
                      ad.to_loc)
      )
    SELECT OrderDropsObj(master_order_no,
                         order_no,
                         po_type,
                         option_id,
                         first_dest,
                         final_dest,
                         handover_date,
                         qty_ordered,
                         unit_cost,
                         ship_method,
                         ship_date,
                         first_dest_date,
                         not_before_date,
                         not_after_date,
                         final_dest_date,
                         freight_forward,
                         ship_port,
                         del_port,
                         supplier_reference,
                         ship_method_final_dest,
                         rev_no)                       
      FROM (SELECT s.master_order_no,
                   s.order_no,
                   s.po_type,
                   s.option_id,
                   s.first_dest,
                   s.final_dest,
                   s.handover_date,
                   s.qty_ordered,
                   s.unit_cost,
                   s.ship_method,
                   s.ship_date,
                   s.first_dest_date,
                   s.not_before_date,
                   s.not_after_date,
                   s.final_dest_date,
                   s.freight_forward,
                   s.ship_port,
                   s.del_port,
                   s.supplier_reference,
                   s.ship_method_final_dest,
                   s.rev_no
              FROM ma_stg_order_drops s,
                   ma_stg_order o
             WHERE o.master_order_no = I_master_order_no
               AND s.master_order_no = o.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT ord.master_order_no,
                   ord.order_no,
                   oh.po_type,
                   ord.option_id,
                   ol.location,
                   NULL final_dest,
                   oh.pickup_date handover_date,
                   ord.qty_ordered,
                   NULL unit_cost,
                   oh.ship_method,
                   oh.not_before_date ship_date,
                   oh.not_after_date first_dest_date,
                   oh.earliest_ship_date not_before_date,
                   oh.latest_ship_date not_after_date,
                   NULL final_dest_date,
                   oh.partner1 freight_forward,
                   NULL ship_port,
                   oh.discharge_port del_port,
                   ord.vpn supplier_reference,
                   NULL ship_method_final_dest,
                   (SELECT rev_no
                      FROM ordhead_rev 
                     WHERE order_no = oh.order_no
                   )rev_no
              FROM (SELECT master_order_no,
                           order_no,
                           CASE
                             WHEN multi_option > 1 THEN
                               TO_CHAR(NULL)
                             ELSE
                               option_id
                           END option_id,
                           supplier,
                           CASE
                             WHEN multi_option > 1 THEN
                               TO_CHAR(NULL)
                             ELSE
                               vpn
                           END vpn,
                           SUM(qty_ordered) qty_ordered
                      FROM (SELECT master_order_no,
                                   order_no,
                                   option_id,
                                   ord.supplier,
                                   os.vpn,
                                   qty_ordered,
                                   (SELECT DISTINCT
                                           COUNT(1) OVER (PARTITION BY ord1.order_no)
                                       FROM ord ord1
                                      WHERE ord1.order_no = ord.order_no
                                      GROUP BY ord1.order_no,
                                               ord1.option_id
                                   )multi_option
                              FROM ord,
                                   (select distinct
                                      supplier,
                                      item,
                                      vpn
                                    from ma_v_option_supplier) os
                             WHERE os.supplier = ord.supplier
                               AND os.item     = ord.option_id
                            )
                    GROUP BY master_order_no,
                             order_no,
                             CASE
                               WHEN multi_option > 1 THEN
                                 TO_CHAR(NULL)
                               ELSE
                                 option_id
                             END,
                             supplier,
                             CASE
                               WHEN multi_option > 1 THEN
                                 TO_CHAR(NULL)
                               ELSE
                                 vpn
                             END                 
                   )ord,
                   (SELECT DISTINCT
                           ol.order_no,
                           ol.location
                      FROM ordloc ol,
                           ordhead oh
                     WHERE ol.order_no     = oh.order_no
                       AND oh.master_po_no = I_master_order_no
                   )ol,
                   ordhead              oh
             WHERE oh.order_no     = ord.order_no
               AND ol.order_no     = ord.order_no
           );
  --
BEGIN
  --
  open C_get_order_drops;
  --
  loop
    --
    fetch C_get_Order_Drops bulk collect into L_OrderDrops limit 100;
    exit when L_OrderDrops.count = 0;
    --
    for i in 1..L_OrderDrops.count loop
      --
      pipe row(OrderDropsObj(L_OrderDrops(i).master_order_no,
                             L_OrderDrops(i).order_no,
                             L_OrderDrops(i).po_type,
                             L_OrderDrops(i).option_id,
                             L_OrderDrops(i).first_dest,
                             L_OrderDrops(i).final_dest,
                             L_OrderDrops(i).handover_date,
                             L_OrderDrops(i).qty_ordered,
                             L_OrderDrops(i).unit_cost,
                             L_OrderDrops(i).ship_method,
                             L_OrderDrops(i).ship_date,
                             L_OrderDrops(i).first_dest_date,
                             L_OrderDrops(i).not_before_date,
                             L_OrderDrops(i).not_after_date,
                             L_OrderDrops(i).final_dest_date,
                             L_OrderDrops(i).freight_forward,
                             L_OrderDrops(i).ship_port,
                             L_OrderDrops(i).del_port,
                             L_OrderDrops(i).supplier_reference,
                             L_OrderDrops(i).ship_method_final_dest,
                             L_OrderDrops(i).rev_no)); 
    --
    end loop;
    --
  end loop;
  --
  close C_get_Order_Drops;
  --
  RETURN;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_ORDER_DROPS',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_ORDER_DROPS;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no IN MA_STG_ORDER_DROPS_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderDropsDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_OrderDropsDetail OrderDropsDetailObjTbl;
  --
  CURSOR C_get_Order_Drops_Detail IS
    WITH ord AS
      (SELECT *
         FROM (SELECT master_po_no master_order_no,
                      order_no,
                      supplier,
                      item_parent option_id,
                      'D' po_type,
                      not_after_date,
                      location loc,
                      SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0)) qty_ordered
                 FROM (SELECT oh.master_po_no,
                              oh.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ol.location,
                              NULL qty_allocated,
                              SUM(ol.qty_ordered) qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              ordloc ol,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.status       = 'A'
                          and ol.order_no     = oh.order_no
                          and ol.item         = im.item
                       GROUP BY oh.master_po_no,
                                oh.order_no,
                                oh.supplier,
                                oh.po_type,
                                trunc(oh.not_after_date),
                                ol.location,
                                im.item_parent
                       UNION ALL
                       SELECT oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ah.wh,
                              sum(ad.qty_allocated),
                              NULL qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              alloc_header ah,
                              alloc_detail ad,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.order_no     = ah.order_no
                          AND ad.alloc_no     = ah.alloc_no
                          AND oh.status       = 'A'
                          AND im.item = ah.item
                     GROUP BY oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date),
                              ah.wh,
                              im.item_parent
                    )
                HAVING (SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0))) > 0
                GROUP BY master_po_no,
                         order_no,
                         supplier,
                         item_parent,
                         not_after_date,
                         LOCATION
              )
              UNION ALL
              (SELECT oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date) not_after_date,
                      ad.to_loc,
                      sum(ad.qty_allocated)
                 FROM ordhead oh,
                      alloc_header ah,
                      alloc_detail ad,
                      ma_v_item_master im
                WHERE oh.master_po_no = I_master_order_no
                  AND oh.order_no     = ah.order_no
                  AND ad.alloc_no     = ah.alloc_no
                  AND oh.status       = 'A'
                  AND im.item = ah.item
             GROUP BY oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date),
                      ad.to_loc)
      )
    SELECT OrderDropsDetailObj(master_order_no,
                               order_no,
                               po_type,
                               option_id,
                               first_dest,
                               final_dest,
                               handover_date,
                               qty_ordered,
                               unit_cost,
                               ship_method,
                               ship_date,
                               first_dest_date,
                               not_before_date,
                               not_after_date,
                               final_dest_date,
                               freight_forward,
                               ship_port,
                               del_port,
                               supplier_reference,
                               ship_method_final_dest)
      FROM (SELECT s.master_order_no,
                   s.order_no,
                   s.po_type,
                   s.option_id,
                   s.first_dest,
                   s.final_dest,
                   s.handover_date,
                   s.qty_ordered,
                   s.unit_cost,
                   s.ship_method,
                   s.ship_date,
                   s.first_dest_date,
                   s.not_before_date,
                   s.not_after_date,
                   s.final_dest_date,
                   s.freight_forward,
                   s.ship_port,
                   s.del_port,
                   s.supplier_reference,
                   s.ship_method_final_dest
              FROM ma_stg_order_drops_detail s,
                   ma_stg_order o
             WHERE o.master_order_no = I_master_order_no
               AND s.master_order_no = o.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT master_order_no,
                   order_no,
                   po_type,
                   option_id,
                   first_dest,
                   final_dest,
                   handover_date,
                   qty_ordered,
                   unit_cost,
                   ship_method,
                   ship_date,
                   first_dest_date,
                   not_before_date,
                   not_after_date,
                   final_dest_date,
                   freight_forward,
                   ship_port,
                   del_port,
                   supplier_reference,
                   ship_method_final_dest
              FROM (SELECT ord.master_order_no,
                           ord.order_no,
                           ord.po_type,
                           ord.option_id,
                           ol.location first_dest,
                           ord.loc final_dest,
                           oh.pickup_date        handover_date,
                           ord.qty_ordered,
                           ol.unit_cost,
                           oh.ship_method,
                           oh.not_before_date    ship_date,
                           oh.not_after_date     first_dest_date,
                           oh.earliest_ship_date not_before_date,
                           oh.latest_ship_date   not_after_date,
                           ord.not_after_date    final_dest_date,
                           oh.partner1           freight_forward,
                           oh.lading_port        ship_port,
                           oh.discharge_port     del_port,
                           os.vpn supplier_reference,
                           MA_ORDER_UTILS_SQL.GET_ORDHEAD_CFA_RMS(I_master_order_no => I_master_order_no,
                                                                  I_order_no        => ord.order_no,
                                                                  I_cfa_type        => 'SHIP_METHOD_FINAL_DEST') ship_method_final_dest
                      FROM ord                  ord,
                           ordhead              oh,
                           (SELECT DISTINCT
                                   ol.order_no,
                                   ol.location,
                                   ol.unit_cost,
                                   im.item_parent
                              FROM ordloc ol,
                                   ordhead oh,
                                   ma_v_item_master im
                             WHERE ol.order_no     = oh.order_no
                               AND oh.master_po_no = I_master_order_no
                               AND im.item         = ol.item
                           )ol,
                           (select distinct
                                      supplier,
                                      item,
                                      vpn
                                    from ma_v_option_supplier) os
                     WHERE oh.order_no     = ord.order_no
                       AND ol.order_no     = ord.order_no
                       AND os.supplier     = ord.supplier
                       AND os.item         = ord.option_id
                       AND ord.option_id   = ol.item_parent
                 )
           );
  --
BEGIN
  --
  open C_get_Order_Drops_Detail;
  --
  loop
    --
    fetch C_get_Order_Drops_Detail bulk collect into L_OrderDropsDetail limit 100;
    exit when L_OrderDropsDetail.count = 0;
    --
    for i in 1..L_OrderDropsDetail.count loop
      --
      pipe row(OrderDropsDetailObj(L_OrderDropsDetail(i).master_order_no,
                                   L_OrderDropsDetail(i).order_no,
                                   L_OrderDropsDetail(i).po_type,
                                   L_OrderDropsDetail(i).option_id,
                                   L_OrderDropsDetail(i).first_dest,
                                   L_OrderDropsDetail(i).final_dest,
                                   L_OrderDropsDetail(i).handover_date,
                                   L_OrderDropsDetail(i).qty_ordered,
                                   L_OrderDropsDetail(i).unit_cost,
                                   L_OrderDropsDetail(i).ship_method,
                                   L_OrderDropsDetail(i).ship_date,
                                   L_OrderDropsDetail(i).first_dest_date,
                                   L_OrderDropsDetail(i).not_before_date,
                                   L_OrderDropsDetail(i).not_after_date,
                                   L_OrderDropsDetail(i).final_dest_date,
                                   L_OrderDropsDetail(i).freight_forward,
                                   L_OrderDropsDetail(i).ship_port,
                                   L_OrderDropsDetail(i).del_port,
                                   L_OrderDropsDetail(i).supplier_reference,
                                   L_OrderDropsDetail(i).ship_method_final_dest)); 
        --
    end loop;
    --
  end loop;
  --
  close C_get_Order_Drops_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_ORDER_DROPS_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_ORDER_DROPS_DETAIL;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_SIZING_OPTION(I_master_order_no IN MA_STG_SIZING_OPTION_DIST.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingOptionObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_OPTION';
  L_error_message VARCHAR2(2000);
  --
  L_sizing_option OrderSizingOptionObjTbl;
  --
  CURSOR C_get_sizing_options IS
    WITH ord AS
      (SELECT *
         FROM (SELECT master_po_no master_order_no,
                      order_no,
                      supplier,
                      item_parent option_id,
                      'D' po_type,
                      not_after_date,
                      location loc,
                      SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0)) qty_ordered
                 FROM (SELECT oh.master_po_no,
                              oh.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ol.location,
                              NULL qty_allocated,
                              SUM(ol.qty_ordered) qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              ordloc ol,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.status       = 'A'
                          and ol.order_no     = oh.order_no
                          and ol.item         = im.item
                       GROUP BY oh.master_po_no,
                                oh.order_no,
                                oh.supplier,
                                oh.po_type,
                                trunc(oh.not_after_date),
                                ol.location,
                                im.item_parent
                       UNION ALL
                       SELECT oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date) not_after_date,
                              ah.wh,
                              sum(ad.qty_allocated),
                              NULL qty_ordered,
                              im.item_parent
                         FROM ordhead oh,
                              alloc_header ah,
                              alloc_detail ad,
                              ma_v_item_master im
                        WHERE oh.master_po_no = I_master_order_no
                          AND oh.order_no     = ah.order_no
                          AND ad.alloc_no     = ah.alloc_no
                          AND oh.status       = 'A'
                          AND im.item = ah.item
                     GROUP BY oh.master_po_no,
                              ah.order_no,
                              oh.supplier,
                              oh.po_type,
                              trunc(oh.not_after_date),
                              ah.wh,
                              im.item_parent
                    )
                HAVING (SUM(NVL(qty_ordered,0)) - SUM(NVL(qty_allocated,0))) > 0
                GROUP BY master_po_no,
                         order_no,
                         supplier,
                         item_parent,
                         not_after_date,
                         LOCATION
              )
              UNION ALL
              (SELECT oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date) not_after_date,
                      ad.to_loc,
                      sum(ad.qty_allocated)
                 FROM ordhead oh,
                      alloc_header ah,
                      alloc_detail ad,
                      ma_v_item_master im
                WHERE oh.master_po_no = I_master_order_no
                  AND oh.order_no     = ah.order_no
                  AND ad.alloc_no     = ah.alloc_no
                  AND oh.status       = 'A'
                  AND im.item = ah.item
             GROUP BY oh.master_po_no,
                      ah.order_no,
                      oh.supplier,
                      im.item_parent,
                      oh.po_type,
                      trunc(ad.in_store_date),
                      ad.to_loc)
      )
    SELECT OrderSizingOptionObj(seq_no,
                                master_order_no,
                                order_no,
                                option_id,
                                final_dest,
                                exp_delivery_date,
                                qty_ordered,
                                sizing_applied,
                                size_profile,
                                size_group,
                                distributed_by,
                                supplier_reference,
                                create_id,
                                create_datetime,
                                last_update_id,
                                last_update_datetime)
      FROM (SELECT od.seq_no,
                   od.master_order_no,
                   od.order_no,
                   od.option_id,
                   od.final_dest,
                   od.exp_delivery_date,
                   od.qty_ordered,
                   od.sizing_applied,
                   od.size_profile,
                   od.size_group,
                   od.distributed_by,
                   od.supplier_reference,
                   od.create_id,
                   od.create_datetime,
                   od.last_update_id,
                   od.last_update_datetime
              FROM ma_stg_sizing_option_dist od,
                   ma_stg_order o
             WHERE o.master_order_no  = I_master_order_no
               AND od.master_order_no = o.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT seq_no,
                   master_order_no,
                   order_no,
                   option_id,
                   final_dest,
                   exp_delivery_date,
                   qty_ordered,
                   sizing_applied,
                   size_profile,
                   size_group,
                   distributed_by,
                   supplier_reference,
                   create_id,
                   create_datetime,
                   last_update_id,
                   last_update_datetime 
              FROM (SELECT NULL seq_no,
                           ord.master_order_no,
                           ord.order_no,
                           ord.option_id,
                           ord.loc final_dest,
                           ord.not_after_date exp_delivery_date,
                           ord.qty_ordered,
                           'Y' sizing_applied,
                           (SELECT DISTINCT
                                   size_profile
                              FROM (SELECT MA_ORDER_UTILS_SQL.GET_ORDSKU_CFA_RMS(I_master_order_no => I_master_order_no,
                                                                                 I_order_no        => ord.order_no,
                                                                                 I_item            => im1.item,
                                                                                 I_cfa_type        => 'SIZE_PROFILE') size_profile
                                       FROM ma_v_item_master im1
                                      WHERE im1.item_parent = ord.option_id
                                        AND ord.po_type = 'D'
                                   )
                              WHERE size_profile IS NOT NULL
                            UNION ALL
                            SELECT DISTINCT
                                   context_value
                              FROM alloc_header ah,
                                   alloc_detail ad,
                                   ma_v_item_master im1
                             WHERE ah.alloc_no     = ad.alloc_no
                               AND ad.to_loc       = ord.loc
                               AND ah.context_type = 'SIZE'
                               AND ah.order_no     = ord.order_no
                               AND ord.po_type <> 'D'
                               AND im1.item_parent = ord.option_id
                               AND ah.item = im1.item
                           )size_profile,
                           im.diff_2 size_group,
                           'Q' distributed_by,
                           os.vpn supplier_reference,
                           oh.create_id,
                           oh.create_datetime,
                           oh.last_update_id,
                           oh.last_update_datetime
                      FROM ord                  ord,
                           ordhead              oh,
                           (select distinct
                                      supplier,
                                      item,
                                      vpn
                                    from ma_v_option_supplier) os,
                           ma_v_item_master     im
                     WHERE oh.order_no     = ord.order_no
                       AND os.supplier     = ord.supplier
                       AND os.item         = ord.option_id
                       AND im.item         = ord.option_id
                   )
           );
  --
BEGIN
  --
  open C_get_sizing_options;
  --
  loop
    --
    fetch C_get_sizing_options bulk collect into L_sizing_option limit 100;
    exit when L_sizing_option.count = 0;
    --
    for i in 1..L_sizing_option.count loop
      --
      pipe row(OrderSizingOptionObj(L_sizing_option(i).seq_no,
                                    L_sizing_option(i).master_order_no,
                                    L_sizing_option(i).order_no,
                                    L_sizing_option(i).option_id,
                                    L_sizing_option(i).final_dest,
                                    L_sizing_option(i).exp_delivery_date,
                                    L_sizing_option(i).qty_ordered,
                                    L_sizing_option(i).sizing_applied,
                                    L_sizing_option(i).size_profile,
                                    L_sizing_option(i).size_group,
                                    L_sizing_option(i).distributed_by,
                                    L_sizing_option(i).supplier_reference,
                                    L_sizing_option(i).create_id,
                                    L_sizing_option(i).create_datetime,
                                    L_sizing_option(i).last_update_id,
                                    L_sizing_option(i).last_update_datetime));
      --
    end loop;
    --
  end loop;
  --
  close C_get_sizing_options;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_SIZING_OPTION',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_SIZING_OPTION;
--------------------------------------------------------------------------------

FUNCTION VIEWMODE_SIZING_DETAILS(I_master_order_no IN MA_V_SIZING_DETAILS.MASTER_ORDER_NO%TYPE)
RETURN OrderSizingDetailsObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_DETAILS';
  L_error_message VARCHAR2(2000);
  --
  L_sizing_details OrderSizingDetailsObjTbl;
  --
  CURSOR C_get_sizing_details IS
    SELECT OrderSizingDetailsObj (sizing_detail_level,
                                  master_order_no,
                                  order_no,
                                  option_id,
                                  final_dest,
                                  exp_delivery_date,
                                  size_code,
                                  size_profile,
                                  sup_ref,
                                  sup_colour,
                                  supp_size_id,
                                  sku,
                                  qty_ordered)
      FROM (select 1 sizing_detail_level,
                   s.master_order_no master_order_no,
                   s.order_no,
                   s.option_id,
                   s.final_dest,
                   s.exp_delivery_date,
                   null size_code,
                   null size_profile,
                   isup.vpn sup_ref,
                   isup.supp_diff_1 sup_colour,
                   null supp_size_id,
                   null sku,
                   null qty_ordered
              from (SELECT * 
                      FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU(I_master_order_no => I_master_order_no))
                   ) s,
                   ma_v_item_supplier isup
             where s.option_id           = isup.item
               and isup.primary_supp_ind = 'Y'
               and s.sku                 is not null
               and s.qty_ordered         is not NULL
             group by s.master_order_no,
                      s.order_no,
                      s.option_id,
                      s.exp_delivery_date,
                      s.final_dest,
                      isup.vpn,
                      isup.supp_diff_1
            UNION ALL
            select 2 sizing_detail_level,
                   s.master_order_no,
                   odist.order_no,
                   s.option_id,
                   s.final_dest,
                   s.exp_delivery_date,
                   s.size_code size_code,
                   odist.size_profile size_profile,
                   isup.vpn sup_ref,
                   null sup_colour,
                   isup.supp_diff_2 supp_size_id,
                   s.sku sku,
                   s.qty_ordered qty_ordered
              from (SELECT * 
                      FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU(I_master_order_no => I_master_order_no))
                   )                         s,
                   ma_v_item_master          i,
                   ma_v_item_supplier        isup,
                   (SELECT * 
                      FROM TABLE(MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_OPTION(I_master_order_no => I_master_order_no))
                   )                         odist
             where s.sku                 is not null
               and s.qty_ordered         is not null
               and s.sku                 = i.item
               and s.sku                 = isup.item
               and isup.primary_supp_ind = 'Y'
               and s.master_order_no     = odist.master_order_no
               and s.option_id           = odist.option_id
               and s.final_dest          = odist.final_dest
               and s.exp_delivery_date   = odist.exp_delivery_date
               and s.order_no            = odist.order_no
           );
  --
BEGIN
  --
  open C_get_sizing_details;
  --
  loop
    --
    fetch C_get_sizing_details bulk collect into L_sizing_details limit 100;
    exit when L_sizing_details.count = 0;
    --
    for i in 1..L_sizing_details.count loop
      --
      pipe row(OrderSizingDetailsObj   (L_sizing_details(i).sizing_detail_level,
                                        L_sizing_details(i).master_order_no,
                                        L_sizing_details(i).order_no,
                                        L_sizing_details(i).option_id,
                                        L_sizing_details(i).final_dest,
                                        L_sizing_details(i).exp_delivery_date,
                                        L_sizing_details(i).size_code,
                                        L_sizing_details(i).size_profile,
                                        L_sizing_details(i).sup_ref,
                                        L_sizing_details(i).sup_colour,
                                        L_sizing_details(i).supp_size_id,
                                        L_sizing_details(i).sku,
                                        L_sizing_details(i).qty_ordered
                                       ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_sizing_details;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_SIZING_DETAILS',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_SIZING_DETAILS;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_DROP_DETAIL(I_master_order_no IN Ma_STG_COST_DROP_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostDropDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_COST_DROP_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_CostDropDetail OrderCostDropDetailObjTbl;
  --
  CURSOR C_get_Cost_Drop_Detail IS
    SELECT OrderCostDropDetailObj (order_level,
                                   master_order_no,
                                   order_no,
                                   fc,
                                   currency_code,
                                   total_unit_cost,
                                   total_discount_cost,
                                   total_expense,
                                   total_duty,
                                   total_landed_cost,
                                   total_retail_value,
                                   exp_buy_margin
                                  )
      FROM (SELECT s.order_level,
                   s.master_order_no,
                   s.order_no,
                   s.fc,
                   s.currency_code,
                   s.total_unit_cost,
                   s.total_discount_cost,
                   s.total_expense,
                   s.total_duty,
                   s.total_landed_cost,
                   s.total_retail_value,
                   s.exp_buy_margin
              FROM ma_stg_cost_drop_detail s,
                   ma_stg_order o
             WHERE s.master_order_no = I_master_order_no
               AND o.master_order_no = s.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT order_level,
                   master_order_no,
                   order_no,
                   fc,
                   currency_code,
                   total_unit_cost,
                   total_discount_cost,
                   total_expense,
                   total_duty,
                   (total_expense + total_duty + total_unit_cost) total_landed_cost,
                   total_retail_value,
                   NULL exp_buy_margin 
              FROM (SELECT order_level,
                           master_order_no,
                           order_no,
                           fc,
                           currency_code,
                           SUM((qty_ordered * unit_cost))    total_unit_cost,
                           0 total_discount_cost,
                           SUM((expense * qty_ordered))      total_expense,
                           0 total_duty,
                           SUM((retail_price * qty_ordered)) total_retail_value
                      FROM (SELECT NULL order_level,
                                   ord.master_order_no,
                                   ord.order_no,
                                   ord.first_dest fc,
                                   wh.currency_code currency_code,
                                   qty_ordered,
                                   (SELECT MAX(unit_cost)
                                      FROM ordloc ol,
                                           ma_v_item_master im
                                     WHERE ol.order_no    = ord.order_no
                                       AND ol.location    = ord.first_dest
                                       AND im.item        = ol.item
                                       AND im.item_parent = ord.option_id
                                   )unit_cost,
                                   (SELECT MAX(unit_retail)
                                      FROM ordloc ol,
                                           ma_v_item_master im
                                     WHERE ol.order_no    = ord.order_no
                                       AND ol.location    = ord.first_dest
                                       AND im.item        = ol.item
                                       AND im.item_parent = ord.option_id
                                   )retail_price,
                                   (SELECT MAX(est_exp_value)
                                      FROM ordloc_exp oe,
                                           ma_v_item_master im
                                     WHERE oe.order_no    = ord.order_no
                                       AND oe.location    = ord.first_dest
                                       AND im.item        = oe.item
                                       AND im.item_parent = ord.option_id
                                       AND oe.comp_id     = 'TEXP'
                                   )expense,
                                   ord.option_id
                              FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no => I_master_order_no)) ord,
                                   wh
                             WHERE wh.wh = ord.first_dest
                               AND NOT EXISTS (SELECT 1 
                                                 FROM ma_stg_order 
                                                WHERE master_order_no = ord.master_order_no 
                                                  AND status IN ('S','W')
                                               )
                           )
                     GROUP BY order_level,
                              master_order_no,
                              order_no,
                              fc,
                              currency_code
                    )
           );
  --
BEGIN
  --
  open C_get_Cost_Drop_Detail;
  --
  loop
    --
    fetch C_get_Cost_Drop_Detail bulk collect into L_CostDropDetail limit 100;
    exit when L_CostDropDetail.count = 0;
    --
    for i in 1..L_CostDropDetail.count loop
      --
      pipe row(OrderCostDropDetailObj   (L_CostDropDetail(i).order_level,
                                          L_CostDropDetail(i).master_order_no,
                                          L_CostDropDetail(i).order_no,
                                          L_CostDropDetail(i).fc,
                                          L_CostDropDetail(i).currency_code,
                                          L_CostDropDetail(i).total_unit_cost,
                                          L_CostDropDetail(i).total_discount_cost,
                                          L_CostDropDetail(i).total_expense,
                                          L_CostDropDetail(i).total_duty,
                                          L_CostDropDetail(i).total_landed_cost,
                                          L_CostDropDetail(i).total_retail_value,
                                          L_CostDropDetail(i).exp_buy_margin
                                        ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_Cost_Drop_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_COST_DROP_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_COST_DROP_DETAIL;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_DUTY_DETAIL(I_master_order_no IN MA_STG_COST_DUTY_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostDutyDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_COST_DUTY_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_CostDutyDetail OrderCostDutyDetailObjTbl;
  --
  CURSOR C_get_Cost_Duty_Detail(v_master_order_no MA_STG_COST_DUTY_DETAIL.MASTER_ORDER_NO%TYPE) IS
    SELECT OrderCostDutyDetailObj (s.duty_id,
                                   s.order_level,
                                   s.master_order_no,
                                   s.order_no,
                                   s.option_id,
                                   s.qty_ordered,
                                   s.fc,
                                   s.fc_desc,
                                   s.supplier_size,
                                   s.sku,
                                   s.cost_component,
                                   s.currency_code,
                                   s.commodity_code,
                                   s.rate,
                                   s.total_value
                                  )
      FROM ma_stg_cost_duty_detail s
     WHERE s.master_order_no=nvl(v_master_order_no, s.master_order_no);
  --
BEGIN
  --
  open C_get_Cost_Duty_Detail(I_master_order_no);
  --
  loop
    --
    fetch C_get_Cost_Duty_Detail bulk collect into L_CostDutyDetail limit 100;
    exit when L_CostDutyDetail.count = 0;
    --
    for i in 1..L_CostDutyDetail.count loop
      --
      pipe row(OrderCostDutyDetailObj   ( L_CostDutyDetail(i).duty_id,
                                          L_CostDutyDetail(i).order_level,
                                          L_CostDutyDetail(i).master_order_no,
                                          L_CostDutyDetail(i).order_no,
                                          L_CostDutyDetail(i).option_id,
                                          L_CostDutyDetail(i).qty_ordered,
                                          L_CostDutyDetail(i).fc,
                                          L_CostDutyDetail(i).fc_desc,
                                          L_CostDutyDetail(i).supplier_size,
                                          L_CostDutyDetail(i).sku,
                                          L_CostDutyDetail(i).cost_component,
                                          L_CostDutyDetail(i).currency_code,
                                          L_CostDutyDetail(i).commodity_code,
                                          L_CostDutyDetail(i).rate,
                                          L_CostDutyDetail(i).total_value
                                        ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_Cost_Duty_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_COST_DUTY_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_COST_DUTY_DETAIL;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_EXPENSE_DETAIL(I_master_order_no IN MA_STG_COST_EXPENSE_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostExpenseDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_COST_EXPENSE_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_CostExpenseDetail OrderCostExpenseDetailObjTbl;
  --
  CURSOR C_get_Cost_Expense_Detail IS
    SELECT OrderCostExpenseDetailObj (expense_id,
                                      order_level,
                                      master_order_no,
                                      order_no,
                                      option_id,
                                      qty_ordered,
                                      fc,
                                      fc_desc,
                                      supplier_size,
                                      sku,
                                      cost_component,
                                      currency_code,
                                      rate,
                                      total_value
                                     )
      FROM (SELECT s.expense_id,
                   s.order_level,
                   s.master_order_no,
                   s.order_no,
                   s.option_id,
                   s.qty_ordered,
                   s.fc,
                   s.fc_desc,
                   s.supplier_size,
                   s.sku,
                   s.cost_component,
                   s.currency_code,
                   s.rate,
                   s.total_value
              FROM ma_stg_cost_expense_detail s,
                   ma_stg_order o
             WHERE s.master_order_no = I_master_order_no
               AND o.master_order_no = s.master_order_no
               AND o.status IN ('S','W')
            UNION ALL
            SELECT NULL expense_id,
                   '1' order_level,
                   ord.master_order_no,
                   ord.order_no,
                   ord.option_id,
                   SUM(ord.qty_ordered) qty_ordered,
                   ord.first_dest fc,
                   wh_name fc_desc,
                   NULL supplier_size,
                   NULL sku,
                   NULL cost_component,
                   NULL currency_code,
                   NULL rate,
                   NULL total_value
              FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no => I_master_order_no)) ord,
                   wh
             WHERE wh.wh = ord.first_dest
               AND NOT EXISTS (SELECT 1 
                                 FROM ma_stg_order 
                                WHERE master_order_no = ord.master_order_no 
                                  AND status IN ('S','W')
                               )
             GROUP BY ord.master_order_no,
                      ord.order_no,
                      ord.option_id,
                      ord.first_dest,
                      wh_name
            UNION ALL
            SELECT NULL expense_id,
                   '2' order_level,
                   ord.master_order_no,
                   ord.order_no,
                   ord.option_id,
                   SUM(ord.qty_ordered) qty_ordered,
                   ol.location fc,
                   wh_name fc_desc,
                   NULL supplier_size,
                   ord.sku,
                   oe.comp_id       cost_component,
                   oe.comp_currency currency_code,
                   oe.comp_rate     rate,
                   SUM(oe.comp_rate * ord.qty_ordered) total_value
              FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU(I_master_order_no => I_master_order_no)) ord,
                   ordloc_exp oe,
                   ordloc ol,
                   wh
             WHERE ol.order_no = ord.order_no
               AND ol.item     = ord.sku
               AND oe.order_no = ord.order_no
               AND oe.item     = ord.sku
               AND oe.location = ol.location
               AND wh.wh       = ol.location
               AND NOT EXISTS (SELECT 1 
                                 FROM ma_stg_order 
                                WHERE master_order_no = ord.master_order_no 
                                  AND status IN ('S','W')
                               )
            GROUP BY ord.master_order_no,
                     ord.order_no,
                     ord.option_id,
                     ol.location,
                     wh_name,
                     ord.sku,
                     oe.comp_id,
                     oe.comp_currency,
                     oe.comp_rate,
                     ord.final_dest
           );
  --
BEGIN
  --
  open C_get_Cost_Expense_Detail;
  --
  loop
    --
    fetch C_get_Cost_Expense_Detail bulk collect into L_CostExpenseDetail limit 100;
    exit when L_CostExpenseDetail.count = 0;
    --
    for i in 1..L_CostExpenseDetail.count loop
      --
      pipe row(OrderCostExpenseDetailObj  ( L_CostExpenseDetail(i).expense_id,
                                            L_CostExpenseDetail(i).order_level,
                                            L_CostExpenseDetail(i).master_order_no,
                                            L_CostExpenseDetail(i).order_no,
                                            L_CostExpenseDetail(i).option_id,
                                            L_CostExpenseDetail(i).qty_ordered,
                                            L_CostExpenseDetail(i).fc,
                                            L_CostExpenseDetail(i).fc_desc,
                                            L_CostExpenseDetail(i).supplier_size,
                                            L_CostExpenseDetail(i).sku,
                                            L_CostExpenseDetail(i).cost_component,
                                            L_CostExpenseDetail(i).currency_code,
                                            L_CostExpenseDetail(i).rate,
                                            L_CostExpenseDetail(i).total_value
                                          ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_Cost_Expense_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_COST_EXPENSE_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_COST_EXPENSE_DETAIL;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_OPTION_DETAIL(I_master_order_no IN MA_STG_COST_OPTION_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostOptionDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_COST_OPTION_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_CostOptionDetail OrderCostOptionDetailObjTbl;
  --
  CURSOR C_get_Cost_Option_Detail IS
    SELECT OrderCostOptionDetailObj ( order_level,
                                      master_order_no,
                                      option_id,
                                      supplier_reference,
                                      order_no,
                                      qty_ordered,
                                      first_dest,
                                      final_dest,
                                      supplier_currency,
                                      unit_cost,
                                      total_unit_cost,
                                      total_discount_cost,
                                      fc_currency,
                                      total_expense,
                                      total_upcharge,
                                      total_duty,
                                      total_landed_cost,
                                      retail_price,
                                      buy_value,
                                      plan_buy_margin,
                                      exp_buy_margin
                                     )
      FROM (SELECT s.order_level,
                   s.master_order_no,
                   s.option_id,
                   s.supplier_reference,
                   s.order_no,
                   s.qty_ordered,
                   s.first_dest,
                   s.final_dest,
                   s.supplier_currency,
                   s.unit_cost,
                   s.total_unit_cost,
                   s.total_discount_cost,
                   s.fc_currency,
                   s.total_expense,
                   s.total_upcharge,
                   s.total_duty,
                   s.total_landed_cost,
                   s.retail_price,
                   s.buy_value,
                   s.plan_buy_margin,
                   s.exp_buy_margin
              FROM ma_stg_cost_option_detail s,
                   ma_stg_order o
             WHERE s.master_order_no = I_master_order_no
               AND o.master_order_no = s.master_order_no
               AND o.status IN ('S','W')
          UNION
          SELECT '1' order_level,
                 ord.master_order_no,
                 ord.option_id,
                 NULL supplier_reference,
                 NULL order_no,
                 sum(qty_ordered) qty_ordered,
                 NULL first_dest,
                 NULL final_dest,
                 NULL supplier_currency,
                 NULL unit_cost,
                 sum(qty_ordered * ol.unit_cost) total_unit_cost,
                 NULL total_discount_cost,
                 NULL fc_currency,
                 NULL total_expense,
                 NULL total_upcharge,
                 NULL total_duty,
                 NULL total_landed_cost,
                 NULL retail_price,
                 NULL buy_value,
                 NULL plan_buy_margin,
                 NULL exp_buy_margin 
            FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no => I_master_order_no)) ord,
                 (SELECT DISTINCT
                         ol.order_no,
                         ol.location,
                         ol.unit_cost
                    FROM ordloc ol,
                         ordhead oh
                   WHERE ol.order_no     = oh.order_no
                     AND oh.master_po_no = I_master_order_no
                     AND oh.status       = 'A'
                 )ol
           WHERE ord.order_no = ol.order_no
           GROUP BY ord.master_order_no,
                    ord.option_id
          UNION ALL
          SELECT order_level,
                 master_order_no,
                 option_id,
                 supplier_reference,
                 order_no,
                 qty_ordered,
                 first_dest,
                 final_dest,
                 supplier_currency,
                 unit_cost,
                 total_unit_cost,
                 total_discount_cost,
                 fc_currency,
                 total_expense,
                 total_upcharge,
                 NULL total_duty,
                 (total_expense + total_upcharge) total_landed_cost,
                 retail_price,
                 buy_value,
                 NULL plan_buy_margin,
                 NULL exp_buy_margin 
            FROM (SELECT order_level,
                         master_order_no,
                         option_id,
                         supplier_reference,
                         order_no,
                         qty_ordered,
                         first_dest,
                         final_dest,
                         supplier_currency,
                         unit_cost,
                         (qty_ordered * unit_cost) total_unit_cost,
                         NULL total_discount_cost,
                         fc_currency,
                         (expense * qty_ordered) total_expense,
                         ((chrg_wh * qty_ordered) + (chrg_ocfrt * qty_ordered)) total_upcharge,
                         retail_price,
                         (retail_price * qty_ordered) buy_value
                    FROM (SELECT '2' order_level,
                                 ord.po_type,
                                 ord.master_order_no,
                                 ord.option_id,
                                 ord.supplier_reference,
                                 ord.order_no,
                                 qty_ordered,
                                 ord.first_dest,
                                 ord.final_dest,
                                 s.currency_code supplier_currency,
                                 (SELECT MAX(unit_cost)
                                    FROM ordloc ol,
                                         ma_v_item_master im
                                   WHERE ol.order_no    = ord.order_no
                                     AND ol.location    = ord.first_dest
                                     AND im.item        = ol.item
                                     AND im.item_parent = ord.option_id
                                 )unit_cost,
                                 wh.currency_code fc_currency,
                                 (SELECT MAX(unit_retail)
                                    FROM ordloc ol,
                                         ma_v_item_master im
                                   WHERE ol.order_no    = ord.order_no
                                     AND ol.location    = ord.first_dest
                                     AND im.item        = ol.item
                                     AND im.item_parent = ord.option_id
                                 )retail_price,
                                 (SELECT MAX(est_exp_value)
                                    FROM ordloc_exp oe,
                                         ma_v_item_master im
                                   WHERE oe.order_no    = ord.order_no
                                     AND oe.location    = ord.first_dest
                                     AND im.item        = oe.item
                                     AND im.item_parent = ord.option_id
                                     AND oe.comp_id     = 'TEXP'
                                 )expense,
                                 NVL((SELECT comp_rate
                                    FROM alloc_chrg ac,
                                         alloc_header ah
                                   WHERE ah.order_no = ord.order_no
                                     AND ac.alloc_no = ah.alloc_no
                                     AND comp_id     = 'WHPROC'
                                     AND to_loc      = ord.final_dest
                                 ),0)chrg_wh,
                                 NVL((SELECT comp_rate
                                    FROM alloc_chrg ac,
                                         alloc_header ah
                                   WHERE ah.order_no = ord.order_no
                                     AND ac.alloc_no = ah.alloc_no
                                     AND comp_id     = 'OCFRT'
                                     AND to_loc      = ord.final_dest
                                 ),0)chrg_ocfrt
                            FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no => I_master_order_no)) ord,
                                 sups s,
                                 ordhead oh,
                                 wh      wh
                           WHERE oh.order_no  = ord.order_no
                             AND s.supplier   = oh.supplier
                             AND wh.wh        = ord.final_dest
                             AND NOT EXISTS (SELECT 1 
                                               FROM ma_stg_order 
                                              WHERE master_order_no = ord.master_order_no 
                                                AND status IN ('S','W')
                                             )
                         )
                  )
            );
  --
BEGIN
  --
  open C_get_Cost_Option_Detail;
  --
  loop
    --
    fetch C_get_Cost_Option_Detail bulk collect into L_CostOptionDetail limit 100;
    exit when L_CostOptionDetail.count = 0;
    --
    for i in 1..L_CostOptionDetail.count loop
      --
      pipe row(OrderCostOptionDetailObj  (  L_CostOptionDetail(i).order_level,
                                            L_CostOptionDetail(i).master_order_no,
                                            L_CostOptionDetail(i).option_id,
                                            L_CostOptionDetail(i).supplier_reference,
                                            L_CostOptionDetail(i).order_no,
                                            L_CostOptionDetail(i).qty_ordered,
                                            L_CostOptionDetail(i).first_dest,
                                            L_CostOptionDetail(i).final_dest,
                                            L_CostOptionDetail(i).supplier_currency,
                                            L_CostOptionDetail(i).unit_cost,
                                            L_CostOptionDetail(i).total_unit_cost,
                                            L_CostOptionDetail(i).total_discount_cost,
                                            L_CostOptionDetail(i).fc_currency,
                                            L_CostOptionDetail(i).total_expense,
                                            L_CostOptionDetail(i).total_upcharge,
                                            L_CostOptionDetail(i).total_duty,
                                            L_CostOptionDetail(i).total_landed_cost,
                                            L_CostOptionDetail(i).retail_price,
                                            L_CostOptionDetail(i).buy_value,
                                            L_CostOptionDetail(i).plan_buy_margin,
                                            L_CostOptionDetail(i).exp_buy_margin
                                          ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_Cost_Option_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_COST_OPTION_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_COST_OPTION_DETAIL;
--------------------------------------------------------------------------------
FUNCTION VIEWMODE_COST_UP_CHARGE_DETAIL(I_master_order_no IN MA_STG_COST_UP_CHARGE_DETAIL.MASTER_ORDER_NO%TYPE)
RETURN OrderCostUpChargeDetailObjTbl PIPELINED IS
  --
  L_program       VARCHAR2(64) := 'MA_ORDER_UTILS_SQL.VIEWMODE_COST_UP_CHARGE_DETAIL';
  L_error_message VARCHAR2(2000);
  --
  L_CostUpChargeDetail OrderCostUpChargeDetailObjTbl;
  --
  CURSOR C_get_Cost_Up_Charge_Detail IS
    SELECT OrderCostUpChargeDetailObj ( order_level,
                                        master_order_no,
                                        option_id,
                                        order_no,
                                        qty_ordered,
                                        first_dest,
                                        final_dest,
                                        supplier_size,
                                        sku,
                                        sku_desc,
                                        cost_component,
                                        currency_code,
                                        rate,
                                        total_value
                                       )
      FROM (SELECT s.order_level,
                   s.master_order_no,
                   s.option_id,
                   s.order_no,
                   s.qty_ordered,
                   s.first_dest,
                   s.final_dest,
                   s.supplier_size,
                   s.sku,
                   s.sku_desc,
                   s.cost_component,
                   s.currency_code,
                   s.rate,
                   s.total_value 
              FROM ma_stg_cost_up_charge_detail s,
                   ma_stg_order o
             WHERE s.master_order_no = I_master_order_no
               AND o.master_order_no = s.master_order_no
               AND o.status IN ('S','W')
            UNION 
            SELECT '1' order_level,
                   ord.master_order_no,
                   ord.option_id,
                   ord.order_no,
                   ord.qty_ordered,
                   ord.first_dest,
                   ord.final_dest,
                   NULL supplier_size,
                   NULL sku,
                   NULL sku_desc,
                   NULL cost_component,
                   NULL currency_code,
                   NULL rate,
                   NULL total_value
              FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_ORDER_DROPS_DETAIL(I_master_order_no => I_master_order_no)) ord
             WHERE ord.first_dest <> ord.final_dest
               AND NOT EXISTS (SELECT 1 
                                 FROM ma_stg_order 
                                WHERE master_order_no = ord.master_order_no 
                                  AND status IN ('S','W')
                               )
            UNION ALL
            SELECT '2' order_level,
                   ord.master_order_no,
                   ord.option_id,
                   ord.order_no,
                   ord.qty_ordered,
                   ol.location first_dest,
                   ord.final_dest,
                   NULL supplier_size,
                   ord.sku,
                   im.item_desc sku_desc,
                   ac.cost_component,
                   ac.currency_code,
                   ac.rate,
                   (ac.rate * ord.qty_ordered) total_value
              FROM table(MA_ORDER_UTILS_SQL.VIEWMODE_SIZING_SKU(I_master_order_no => I_master_order_no)) ord,
                   (SELECT ol.order_no,
                           ol.location,
                           ol.unit_cost,
                           ol.item
                      FROM ordloc ol,
                           ordhead oh
                     WHERE ol.order_no     = oh.order_no
                       AND oh.master_po_no = I_master_order_no
                   )ol,
                   ma_v_item_master im,
                   (SELECT comp_id       cost_component,
                           comp_currency currency_code,
                           comp_rate rate,
                           ac.item,
                           ac.to_loc
                      FROM alloc_chrg ac,
                           alloc_header ah,
                           ordhead oh
                     WHERE ah.order_no = oh.order_no
                       AND ac.alloc_no = ah.alloc_no
                       AND oh.master_po_no = I_master_order_no
                   )ac
             WHERE ord.order_no = ol.order_no
               AND ol.item      = ord.sku
               AND ol.location <> ord.final_dest
               AND im.item      = ord.sku
               AND ac.item      = ord.sku
               AND ac.to_loc    = ord.final_dest
         );
  --
BEGIN
  --
  open C_get_Cost_Up_Charge_Detail;
  --
  loop
    --
    fetch C_get_Cost_Up_Charge_Detail bulk collect into L_CostUpChargeDetail limit 100;
    exit when L_CostUpChargeDetail.count = 0;
    --
    for i in 1..L_CostUpChargeDetail.count loop
      --
      pipe row(OrderCostUpChargeDetailObj  (  L_CostUpChargeDetail(i).order_level,
                                              L_CostUpChargeDetail(i).master_order_no,
                                              L_CostUpChargeDetail(i).option_id,
                                              L_CostUpChargeDetail(i).order_no,
                                              L_CostUpChargeDetail(i).qty_ordered,
                                              L_CostUpChargeDetail(i).first_dest,
                                              L_CostUpChargeDetail(i).final_dest,
                                              L_CostUpChargeDetail(i).supplier_size,
                                              L_CostUpChargeDetail(i).sku,
                                              L_CostUpChargeDetail(i).sku_desc,
                                              L_CostUpChargeDetail(i).cost_component,
                                              L_CostUpChargeDetail(i).currency_code,
                                              L_CostUpChargeDetail(i).rate,
                                              L_CostUpChargeDetail(i).total_value
                                           ));
        --
    end loop;
    --
  end loop;
  --
  close C_get_Cost_Up_Charge_Detail;
  --
  return;
  --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => MICROAPP_ID,
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_VIEWMODE_COST_UP_CHARGE_DETAIL',
                                              I_aux_1             => I_master_order_no,
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
  --
END VIEWMODE_COST_UP_CHARGE_DETAIL;
--------------------------------------------------------------------------------
FUNCTION REPLENISHMENT_SEARCH(I_option_id            IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                              I_supplier             IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                              I_fc                   IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                              I_dept_list            IN  VARCHAR2 DEFAULT NULL,
                              I_class_list           IN  VARCHAR2 DEFAULT NULL,
                              I_subclass_list        IN  VARCHAR2 DEFAULT NULL,
                              I_business_model_list  IN  VARCHAR2 DEFAULT NULL,
                              I_buying_group_list    IN  VARCHAR2 DEFAULT NULL,
                              I_handover_date        IN  VARCHAR2 DEFAULT NULL,
                              I_execute_query        IN  VARCHAR2 DEFAULT 'N')
RETURN PoReplenSearchTbl PIPELINED AS

 --
 L_program               VARCHAR2(64) := 'REPLENISHMENT_SEARCH';
 L_error_message         VARCHAR2(2000);
 L_PoReplenSearchTbl     PoReplenSearchTbl;
 L_string_query          VARCHAR2(20000);
 L_sys_refcur            SYS_REFCURSOR;
 --
BEGIN
  --
  if I_execute_query = 'N' then
    --
    RETURN;
    --
  end if;
  --
  EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_DATE_FORMAT = ''DD-MM-YYYY''');
  --
  -- build query string
  --
  L_string_query := q'{with t_binds as
                            (select :1 bv_item,
                                    :2 bv_supplier,
                                    :3 bv_fc,
                                    :4 bv_dept_list,
                                    :5 bv_class_list,
                                    :6 bv_subclass_list,
                                    :7 bv_business_model_list,
                                    :8 bv_buying_group_list,
                                    :9 bv_handover_date
                                from dual)
                             select /*+ result_cache */ 
                                    new PoReplenSearchObj(order_level,
                                                          parent,
                                                          item_desc,
                                                          supp_ref,
                                                          supplier,
                                                          supp_desc,
                                                          item,
                                                          size_code,
                                                          qty_ordered,
                                                          primary_repl_supplier,
                                                          location,
                                                          fc_name,
                                                          loc_type,
                                                          need_date,
                                                          product_group,
                                                          category,
                                                          sub_category,
                                                          business_model,
                                                          buying_group
                                                         ) 
                                  from MA_V_REPLENISHMENT i,
                                       t_binds b
                                 where 1 = 1
                       }';

  --
  -- item
  --
  if I_option_id is not null then
    --
    L_string_query := L_string_query || q'{ and ( 
                                                  i.parent = b.bv_item 
                                                  or 
                                                  (
                                                    (
                                                      i.item = b.bv_item 
                                                      and 
                                                      i.parent = (select parent 
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item)
                                                      and 
                                                      i.location = (select location 
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item)
                                                      and
                                                       i.need_date = (select need_date
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item)
                                                    )
                                                    or
                                                    (
                                                      i.parent = (select parent 
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item) 
                                                      and 
                                                      i.location = (select location 
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item)
                                                      and
                                                       i.need_date = (select need_date
                                                                    from MA_V_REPLENISHMENT 
                                                                   where item = b.bv_item)
                                                      and 
                                                      i.item is null
                                                    )
                                                  )
                                                ) }';
    --
  end if; 
  --
  -- supplier
  --
  if I_supplier is not null then
    --
    L_string_query := L_string_query || q'{ and (i.primary_repl_supplier = b.bv_supplier ) }';
    --
  end if;
  --
  -- handover_date
  --
  if I_handover_date is not null then
    --
    L_string_query := L_string_query || q'{ and trunc(i.need_date) = trunc(to_date(b.bv_handover_date,'DD-MM-YYYY'))
                                          }';
    --
  end if;
  --
  -- location
  --
  if I_fc is not null then
    --
    L_string_query := L_string_query || q'{ and (i.location = b.bv_fc ) }';
    --
  end if;
  --
  -- dept,class,subclass
  --
  if I_subclass_list is not null then
    --
    L_string_query := L_string_query ||  q'{ and exists (select 1
                                                           from (
                                                                select TRIM(LEADING '0' FROM substr(column_value,1,4)) product_group,
                                                                       TRIM(LEADING '0' FROM substr(column_value,5,4)) category,
                                                                       TRIM(LEADING '0' FROM substr(column_value,9,4)) sub_category
                                                                  from table(convert_comma_list(b.bv_subclass_list))
                                                                 )
                                                          where product_group = i.product_group
                                                            and category      = i.category
                                                            and sub_category  = i.sub_category
                                                          )
                                          }';
    --
  elsif I_class_list is not null then
    --
    L_string_query := L_string_query ||  q'{ and exists (select 1
                                                           from (
                                                                select TRIM(LEADING '0' FROM substr(column_value,1,4)) product_group,
                                                                       TRIM(LEADING '0' FROM substr(column_value,5,4)) category
                                                                  from table(convert_comma_list(b.bv_class_list))
                                                                 )
                                                          where product_group = i.product_group
                                                            and category      = i.category
                                                          )
                                          }';
    --
  elsif I_dept_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                          from table(convert_comma_list(b.bv_dept_list))
                                                         where column_value = i.product_group
                                                        )
                                          }';
    --

  end if;
  --
  -- business_model, buying_group
  --
  if I_buying_group_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                         from (
                                                               select TRIM(LEADING '0' FROM substr(column_value,1,4)) business_model,
                                                                      TRIM(LEADING '0' FROM substr(column_value,5,4)) buying_group
                                                                 from table(convert_comma_list(b.bv_buying_group_list))
                                                               )
                                                        where business_model = i.business_model
                                                          and buying_group   = i.buying_group
                                                          )
                                          }';
    --
  elsif I_business_model_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                          from table(convert_comma_list(b.bv_business_model_list))
                                                         where column_value = i.business_model
                                                        )
                                          }';
    --
  end if;
  --
  --dbms_output.put_line(L_string_query);
  --
  -- bulk query to table type
  --
  open L_sys_refcur for L_string_query using I_option_id,
                                             I_supplier,
                                             I_fc,
                                             I_dept_list,
                                             I_class_list,
                                             I_subclass_list,
                                             I_business_model_list,
                                             I_buying_group_list,
                                             I_handover_date ;
  loop
    --
    fetch L_sys_refcur bulk collect into L_PoReplenSearchTbl limit 100;
    exit when L_PoReplenSearchTbl.count = 0;
    --
    -- pipe date from collection
    --
    for i in 1..L_PoReplenSearchTbl.count loop
      --
      pipe row (PoReplenSearchObj( L_PoReplenSearchTbl(i).order_level,
                                   L_PoReplenSearchTbl(i).parent,
                                   L_PoReplenSearchTbl(i).item_desc,
                                   L_PoReplenSearchTbl(i).supp_ref,
                                   L_PoReplenSearchTbl(i).supplier,
                                   L_PoReplenSearchTbl(i).supp_desc,
                                   L_PoReplenSearchTbl(i).item,
                                   L_PoReplenSearchTbl(i).size_code,
                                   L_PoReplenSearchTbl(i).qty_ordered,
                                   L_PoReplenSearchTbl(i).primary_repl_supplier,
                                   L_PoReplenSearchTbl(i).location,
                                   L_PoReplenSearchTbl(i).fc_name,
                                   L_PoReplenSearchTbl(i).loc_type,
                                   L_PoReplenSearchTbl(i).need_date,
                                   L_PoReplenSearchTbl(i).product_group,
                                   L_PoReplenSearchTbl(i).category,
                                   L_PoReplenSearchTbl(i).sub_category,
                                   L_PoReplenSearchTbl(i).business_model,
                                   L_PoReplenSearchTbl(i).buying_group
                                    ));
       --
    end loop;
    --
   end loop;
   --
   CLOSE L_sys_refcur;
   --
   RETURN;
   --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => 'ORDERS',
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_REPLEN_SEARCH',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
 --
END REPLENISHMENT_SEARCH;
--------------------------------------------------------------------------------
FUNCTION PLANNING_SEARCH( I_option_id            IN  MA_STG_ORDER_OPTION.OPTION_ID%TYPE DEFAULT NULL,
                          I_supplier             IN  MA_STG_ORDER.SUPPLIER%TYPE DEFAULT NULL,
                          I_fc                   IN  MA_STG_ORDER_DROPS.FIRST_DEST%TYPE DEFAULT NULL,
                          I_dept_list            IN  VARCHAR2 DEFAULT NULL,
                          I_class_list           IN  VARCHAR2 DEFAULT NULL,
                          I_subclass_list        IN  VARCHAR2 DEFAULT NULL,
                          I_business_model_list  IN  VARCHAR2 DEFAULT NULL,
                          I_buying_group_list    IN  VARCHAR2 DEFAULT NULL,
                          I_execute_query        IN  VARCHAR2 DEFAULT 'N')
RETURN PoPlannSearchTbl PIPELINED AS

 --
 L_program               VARCHAR2(64) := 'PLANNING_SEARCH';
 L_error_message         VARCHAR2(2000);
 L_PoPlannSearchTbl      PoPlannSearchTbl;
 L_string_query          VARCHAR2(20000);
 L_sys_refcur            SYS_REFCURSOR;
 --
BEGIN
  --
  if I_execute_query = 'N' then
    --
    RETURN;
    --
  end if;
  --
  -- build query string
  --
  L_string_query := q'{with t_binds as
                            (select :1 bv_item,
                                    :2 bv_supplier,
                                    :3 bv_fc,
                                    :4 bv_dept_list,
                                    :5 bv_class_list,
                                    :6 bv_subclass_list,
                                    :7 bv_business_model_list,
                                    :8 bv_buying_group_list
                                from dual)
                             select /*+ result_cache */ 
                                    new PoPlannSearchObj(process_id,
                                                         order_level,
                                                         order_rec_no,
                                                         po_type,
                                                         option_id,
                                                         item_desc,
                                                         qty_ordered,
                                                         supplier,
                                                         factory,
                                                         supp_desc,
                                                         first_dest,
                                                         final_dest,
                                                         fc_name,
                                                         loc_type,
                                                         size_profile,
                                                         handover_date,
                                                         not_before_date,
                                                         not_after_date,
                                                         create_datetime,
                                                         last_update_datetime,
                                                         create_id,
                                                         last_update_id,
                                                         supp_ref,
                                                         product_group,
                                                         category,
                                                         sub_category,
                                                         business_model,
                                                         buying_group,
                                                         sku_id,
                                                         size_code) 
                                  from MA_V_PLANNING i,
                                       t_binds b
                                 where 1 = 1
                       }';

  --
  -- item
  --
  if I_option_id is not null then
    --
    L_string_query := L_string_query || q'{ and ( 
                                                  i.option_id = b.bv_item 
                                                  or 
                                                  (
                                                    (
                                                      i.sku_id = b.bv_item 
                                                      and 
                                                      i.option_id = (select option_id 
                                                                    from MA_V_PLANNING 
                                                                   where sku_id = b.bv_item)
                                                      and 
                                                      i.final_dest = (select final_dest 
                                                                    from MA_V_PLANNING 
                                                                   where sku_id = b.bv_item)
                                                       and
                                                       i.handover_date = (select handover_date
                                                                          from MA_V_PLANNING 
                                                                          where sku_id = b.bv_item)             
                                                    )
                                                    or
                                                    (
                                                      i.option_id = (select option_id 
                                                                    from MA_V_PLANNING 
                                                                   where sku_id = b.bv_item) 
                                                      and 
                                                      i.final_dest = (select final_dest 
                                                                    from MA_V_PLANNING 
                                                                   where sku_id = b.bv_item)
                                                      and
                                                       i.handover_date = (select handover_date
                                                                          from MA_V_PLANNING 
                                                                          where sku_id = b.bv_item)
                                                      and 
                                                      i.sku_id is null
                                                    )
                                                  )
                                                ) }';
    --
  end if; 
  --
  -- supplier
  --
  if I_supplier is not null then
    --
    L_string_query := L_string_query || q'{ and (i.supplier = b.bv_supplier ) }';
    --
  end if;
  --
  -- location
  --
  if I_fc is not null then
    --
    L_string_query := L_string_query || q'{ and (i.final_dest = b.bv_fc ) }';
    --
  end if;
  --
  -- dept,class,subclass
  --
  if I_subclass_list is not null then
    --
    L_string_query := L_string_query ||  q'{ and exists (select 1
                                                           from (
                                                                select TRIM(LEADING '0' FROM substr(column_value,1,4)) product_group,
                                                                       TRIM(LEADING '0' FROM substr(column_value,5,4)) category,
                                                                       TRIM(LEADING '0' FROM substr(column_value,9,4)) sub_category
                                                                  from table(convert_comma_list(b.bv_subclass_list))
                                                                 )
                                                          where product_group = i.product_group
                                                            and category      = i.category
                                                            and sub_category  = i.sub_category
                                                          )
                                          }';
    --
  elsif I_class_list is not null then
    --
    L_string_query := L_string_query ||  q'{ and exists (select 1
                                                           from (
                                                                select TRIM(LEADING '0' FROM substr(column_value,1,4)) product_group,
                                                                       TRIM(LEADING '0' FROM substr(column_value,5,4)) category
                                                                  from table(convert_comma_list(b.bv_class_list))
                                                                 )
                                                          where product_group = i.product_group
                                                            and category      = i.category
                                                          )
                                          }';
    --
  elsif I_dept_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                          from table(convert_comma_list(b.bv_dept_list))
                                                         where column_value = i.product_group
                                                        )
                                          }';
    --

  end if;
  --
  -- business_model, buying_group
  --
  if I_buying_group_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                         from (
                                                               select TRIM(LEADING '0' FROM substr(column_value,1,4)) business_model,
                                                                      TRIM(LEADING '0' FROM substr(column_value,5,4)) buying_group
                                                                 from table(convert_comma_list(b.bv_buying_group_list))
                                                               )
                                                        where business_model = i.business_model
                                                          and buying_group   = i.buying_group
                                                          )
                                          }';
    --
  elsif I_business_model_list is not null then
    --
    L_string_query := L_string_query || q'{ and exists (select 1
                                                          from table(convert_comma_list(b.bv_business_model_list))
                                                         where column_value = i.business_model
                                                        )
                                          }';
    --
  end if;
  --
  --dbms_output.put_line(L_string_query);
  --
  -- bulk query to table type
  --
  open L_sys_refcur for L_string_query using I_option_id,
                                             I_supplier,
                                             I_fc,
                                             I_dept_list,
                                             I_class_list,
                                             I_subclass_list,
                                             I_business_model_list,
                                             I_buying_group_list ;
  loop
    --
    fetch L_sys_refcur bulk collect into L_PoPlannSearchTbl limit 100;
    exit when L_PoPlannSearchTbl.count = 0;
    --
    -- pipe date from collection
    --
    for i in 1..L_PoPlannSearchTbl.count loop
      --
      pipe row (PoPlannSearchObj(L_PoPlannSearchTbl(i).process_id,
                                 L_PoPlannSearchTbl(i).order_level,
                                 L_PoPlannSearchTbl(i).order_rec_no,
                                 L_PoPlannSearchTbl(i).po_type,
                                 L_PoPlannSearchTbl(i).option_id,
                                 L_PoPlannSearchTbl(i).item_desc,
                                 L_PoPlannSearchTbl(i).qty_ordered,
                                 L_PoPlannSearchTbl(i).supplier,
                                 L_PoPlannSearchTbl(i).factory,
                                 L_PoPlannSearchTbl(i).supp_desc,
                                 L_PoPlannSearchTbl(i).first_dest,
                                 L_PoPlannSearchTbl(i).final_dest,
                                 L_PoPlannSearchTbl(i).fc_name,
                                 L_PoPlannSearchTbl(i).loc_type,
                                 L_PoPlannSearchTbl(i).size_profile,
                                 L_PoPlannSearchTbl(i).handover_date,
                                 L_PoPlannSearchTbl(i).not_before_date,
                                 L_PoPlannSearchTbl(i).not_after_date,
                                 L_PoPlannSearchTbl(i).create_datetime,
                                 L_PoPlannSearchTbl(i).last_update_datetime,
                                 L_PoPlannSearchTbl(i).create_id,
                                 L_PoPlannSearchTbl(i).last_update_id,
                                 L_PoPlannSearchTbl(i).supp_ref,
                                 L_PoPlannSearchTbl(i).product_group,
                                 L_PoPlannSearchTbl(i).category,
                                 L_PoPlannSearchTbl(i).sub_category,
                                 L_PoPlannSearchTbl(i).business_model,
                                 L_PoPlannSearchTbl(i).buying_group,
                                 L_PoPlannSearchTbl(i).sku_id,
                                 L_PoPlannSearchTbl(i).size_code
                                 )
       );
       --
    end loop;
    --
   end loop;
   --
   CLOSE L_sys_refcur;
   --
   RETURN;
   --
EXCEPTION
  --
  WHEN NO_DATA_NEEDED THEN
    --
    RETURN;
    --
  WHEN OTHERS THEN
    --
    L_error_message := LOG_SQL.HANDLE_MA_LOGS(I_ma_id             => 'ORDERS',
                                              I_log_level         => GLOBAL_VARS_SQL.G_level_error,
                                              I_program_name      => L_program,
                                              I_error_key         => 'ERROR_PLANN_SEARCH',
                                              I_error_backtrace   => dbms_utility.format_error_backtrace,
                                              I_error_stack       => dbms_utility.format_error_stack);
    --
    dbms_output.put_line(L_error_message);
    --
    RETURN;
    --
 --
END PLANNING_SEARCH;
--------------------------------------------------------------------------------

BEGIN
  --
  EXECUTE IMMEDIATE ('ALTER SESSION SET NLS_DATE_FORMAT = ''DD-MM-YYYY''');
  --
 END MA_ORDER_UTILS_SQL_LHB;