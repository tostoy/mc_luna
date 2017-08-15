-- 订单表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order LIKE rmdc_dw.odps_user_loan_order;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order PARTITION (pt)
SELECT 
a.id, user_id, order_type, money_amount, apr, loan_method, loan_term, loan_interests, operator_name, remark, created_at, updated_at, status, order_time, loan_time, trail_time, current_interests, late_fee, late_fee_apr, card_id, counter_fee, reason_remark, is_first, auto_risk_check_status, is_hit_risk_rule, sub_order_type, is_user_confirm, from_app, coupon_id, tree, fund_id, card_type, temp_check_status, post_interest, pt 
FROM rmdc_dw.odps_user_loan_order a
JOIN (
 SELECT id FROM 
  (
	  SELECT pt, id, ROW_NUMBER() OVER (PARTITION BY pt ORDER BY id ASC) AS x_count FROM rmdc_dw.odps_user_loan_order WHERE pt >="20170601" AND pt <= "20170814"
  ) table001 
  WHERE x_count <= 1000
) b
ON a.id = b.id;


-- 还款表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_repayment LIKE rmdc_dw.odps_user_loan_order_repayment;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_repayment PARTITION (pt)
SELECT 
a.id, user_id, order_id, principal, interests, late_day, late_fee, plan_repayment_time, plan_fee_time, operator_name, status, remark, created_at, updated_at, total_money, card_id, true_repayment_time, loan_time, interest_day, apr, loan_day, apply_repayment_time, interest_time, true_total_money, debit_times, current_debit_money, is_overdue, overdue_day, coupon_id, coupon_money, user_book, pt
FROM rmdc_dw.odps_user_loan_order_repayment a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 借款人表
CREATE TABLE rmdc_dw_test1.odps_loan_person LIKE rmdc_dw.odps_loan_person;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_loan_person PARTITION (pt)
SELECT 
a.id, uid, open_id, id_number, type, name, phone, birthday, property, contact_username, attachment, credit_limit, created_at, updated_at, source_id, contact_phone, is_verify, created_ip, auth_key, invite_code, status, username, card_bind_status, customer_type, can_loan_time, pt 
FROM rmdc_dw.odps_loan_person a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.id = b.user_id;


-- 订单属性表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_attribute LIKE rmdc_dw.odps_user_loan_order_attribute;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_attribute PARTITION (pt)
SELECT 
a.order_id, user_id, check_type, after_status, checked_at, tree, operator_name, created_at, updated_at, is_active, rank_data, is_new, amount, pt 
FROM rmdc_dw.odps_user_loan_order_attribute a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 用户认证表
CREATE TABLE rmdc_dw_test1.odps_user_verification LIKE rmdc_dw.odps_user_verification;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_verification PARTITION (pt)
SELECT 
id, a.user_id, real_pay_pwd_status, real_verify_status, real_work_status, real_contact_status, real_bind_bank_card_status, real_zmxy_status, updated_at, created_at, operator_name, remark, status, is_quota_novice, is_fzd_novice, real_work_fzd_status, real_credit_card_status, is_first_loan, real_jxl_status, real_more_status, real_alipay_status, real_yys_status, real_taobao_status, real_jd_status, real_accredit_status, real_online_bank_status, real_wy_status, real_social_security_status, pt 
FROM rmdc_dw.odps_user_verification a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 用户认证扩展表
CREATE TABLE rmdc_dw_test1.odps_user_verification_extend LIKE rmdc_dw.odps_user_verification_extend;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_verification_extend PARTITION (pt)
SELECT 
id, a.user_id, real_accumulation_fund, real_social_security, real_status, updated_at, created_at, golden_tag, is_white, card_type, authentication_a, authentication_b, authentication_c, authentication_d, authentication_e, authentication_f, authentication_h, authentication_i, authentication_j, authentication_k, real_credit_card, real_card_bill_online, real_card_bill_email, pt 
FROM rmdc_dw.odps_user_verification_extend a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 员工贷展期记录表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_delay_log LIKE rmdc_dw.odps_user_loan_order_delay_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_delay_log PARTITION (pt)
SELECT 
a.id, a.order_id, user_id, service_fee, counter_fee, late_fee, delay_day, principal, status, coupon_id, coupon_amount, remark, created_at, updated_at, repay_account_id, late_day, plan_fee_time, pt 
FROM rmdc_dw.odps_user_loan_order_delay_log a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 借款审核表
CREATE TABLE rmdc_dw_test1.odps_user_order_loan_check_log LIKE rmdc_dw.odps_user_order_loan_check_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_order_loan_check_log PARTITION (pt)
SELECT 
a.id, a.order_id, user_id, repayment_id, before_status, after_status, operator_name, remark, type, operation_type, repayment_type, created_at, updated_at, head_code, back_code, reason_remark, can_loan_type, tree, pt 
FROM rmdc_dw.odps_user_order_loan_check_log a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 用户每日签到表
CREATE TABLE rmdc_dw_test1.odps_user_checked_date LIKE rmdc_dw.odps_user_checked_date;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_checked_date PARTITION (pt)
SELECT 
id, a.user_id, last_check_date, total_check_num, series_check_num, is_auto_reminder, created_at, updated_at, pt 
FROM rmdc_dw.odps_user_checked_date a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 提升额度个人信息表
CREATE TABLE rmdc_dw_test1.odps_user_quota_person_info LIKE rmdc_dw.odps_user_quota_person_info;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_quota_person_info PARTITION (pt)
SELECT 
id, a.user_id, address, longitude, latitude, live_time_type, degrees, marriage, status, created_at, updated_at, address_distinct, pt 
FROM rmdc_dw.odps_user_quota_person_info a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 额度个人信息表
CREATE TABLE rmdc_dw_test1.odps_user_credit_total LIKE rmdc_dw.odps_user_credit_total;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_credit_total PARTITION (pt)
SELECT 
id, a.user_id, amount, used_amount, locked_amount, updated_at, created_at, operator_name, remark, pocket_apr, house_apr, installment_apr, pocket_late_apr, house_late_apr, installment_late_apr, pocket_min, pocket_max, house_min, house_max, installment_min, installment_max, card_type, card_title, card_subtitle, card_no, initial_amount, is_already_initial_amount, increase_time, repayment_credit_add, pt 
FROM rmdc_dw.odps_user_credit_total a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;



-- 用户额度审核表 -- 用户额度修改流水表
CREATE TABLE rmdc_dw_test1.odps_user_credit_review_log LIKE rmdc_dw.odps_user_credit_review_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_credit_review_log PARTITION (pt)
SELECT 
id, a.user_id, type, before_number, operate_number, after_number, status, creater_name, operator_name, remark, created_at, pt 
FROM rmdc_dw.odps_user_credit_review_log a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;



-- 用户注册信息表
CREATE TABLE rmdc_dw_test1.odps_user_register_info LIKE rmdc_dw.odps_user_register_info;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_register_info PARTITION (pt)
SELECT 
id, a.user_id, clientType, osVersion, appVersion, deviceName, created_at, appMarket, deviceId, x_date, source, pt 
FROM rmdc_dw.odps_user_register_info a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 借款分期订单
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_period LIKE rmdc_dw.odps_user_loan_order_period;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_period PARTITION (pt)
SELECT 
id, a.user_id, order_type, money_amount, apr, loan_method, loan_term, loan_interests, operator_name, remark, created_at, updated_at, status, order_time, loan_time, trail_time, current_interests, late_fee, late_fee_apr, card_id, counter_fee, reason_remark, is_first, auto_risk_check_status, is_hit_risk_rule, sub_order_type, is_user_confirm, from_app, coupon_id, tree, fund_id, card_type, temp_check_status, pt 
FROM rmdc_dw.odps_user_loan_order_period a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 用户属性表
CREATE TABLE rmdc_dw_test1.odps_user_attribute LIKE rmdc_dw.odps_user_attribute;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_attribute PARTITION (pt)
SELECT 
a.user_id, created_at, updated_at, rank_data, type, flag, is_blacklist, pt 
FROM rmdc_dw.odps_user_attribute a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 还款订单
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_repayment_period LIKE rmdc_dw.odps_user_loan_order_repayment_period;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_repayment_period PARTITION (pt)
SELECT 
id, a.user_id, order_id, period, principal, interests, late_day, late_fee, plan_repayment_time, plan_fee_time, operator_name, status, total_money, card_id, true_repayment_time, loan_time, interest_day, apr, loan_day, apply_repayment_time, interest_time, true_total_money, debit_times, current_debit_money, is_overdue, overdue_day, coupon_id, coupon_money, user_book, remark, created_at, updated_at, pt 
FROM rmdc_dw.odps_user_loan_order_repayment_period a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;



-- 近期用户额度个人信息表
CREATE TABLE rmdc_dw_test1.odps_period_user_credit_total LIKE rmdc_dw.odps_period_user_credit_total;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_period_user_credit_total PARTITION (pt)
SELECT 
id, a.user_id, amount, used_amount, locked_amount, updated_at, created_at, operator_name, remark, pocket_apr, house_apr, installment_apr, pocket_late_apr, house_late_apr, installment_late_apr, pocket_min, pocket_max, house_min, house_max, installment_min, installment_max, card_type, card_title, card_subtitle, card_no, initial_amount, is_already_initial_amount, increase_time, repayment_credit_add, pt 
FROM rmdc_dw.odps_period_user_credit_total a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 现金卡部分还款记录表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_partial LIKE rmdc_dw.odps_user_loan_order_partial;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_partial PARTITION (pt)
SELECT 
a.id, a.order_id, user_id, repayment_amount, last_amount, service_fee, total_fee, delay_times, late_fee, last_id, remark, created_at, updated_at, pt 
FROM rmdc_dw.odps_user_loan_order_partial a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;



-- 用户借款日志信息表
CREATE TABLE rmdc_dw_test1.odps_user_credit_money_log LIKE rmdc_dw.odps_user_credit_money_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_credit_money_log PARTITION (pt)
SELECT 
a.id, type, payment_type, status, user_id, a.order_id, order_uuid, operator_money, operator_name, pay_order_id, created_at, remark, img_url, updated_at, card_id, debit_channel, debit_account, pt 
FROM rmdc_dw.odps_user_credit_money_log a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;



-- 借款审核表
CREATE TABLE rmdc_dw_test1.odps_period_user_order_loan_check_log LIKE rmdc_dw.odps_period_user_order_loan_check_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_period_user_order_loan_check_log PARTITION (pt)
SELECT 
a.id, a.order_id, user_id, repayment_id, before_status, after_status, operator_name, remark, type, operation_type, repayment_type, created_at, updated_at, head_code, back_code, reason_remark, can_loan_type, tree, pt 
FROM rmdc_dw.odps_period_user_order_loan_check_log a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 现金卡部分还款日志表
CREATE TABLE rmdc_dw_test1.odps_user_loan_order_partial_log LIKE rmdc_dw.odps_user_loan_order_partial_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_loan_order_partial_log PARTITION (pt)
SELECT 
a.id, a.order_id, user_id, repayment_amount, last_amount, service_fee, total_fee, delay_times, late_fee, repay_account_id, remark, created_at, updated_at, status, pt 
FROM rmdc_dw.odps_user_loan_order_partial_log a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.order_id = b.id;


-- 社保信息表
CREATE TABLE rmdc_dw_test1.odps_social_security LIKE rmdc_dw.odps_social_security;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_social_security PARTITION (pt)
SELECT 
id, a.user_id, city, status, message, x_data, created_at, updated_at, open_id, pt 
FROM rmdc_dw.odps_social_security a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 公积金信息表
CREATE TABLE rmdc_dw_test1.odps_accumulation_fund LIKE rmdc_dw.odps_accumulation_fund;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_accumulation_fund PARTITION (pt)
SELECT 
id, a.user_id, city, status, x_data, message, created_at, updated_at, open_id, pt 
FROM rmdc_dw.odps_accumulation_fund a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 银行卡信息
CREATE TABLE rmdc_dw_test1.odps_online_bank_info LIKE rmdc_dw.odps_online_bank_info;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_online_bank_info PARTITION (pt)
SELECT 
id, a.user_id, bank_id, bank_name, status, open_id, x_data, bank_num, created_at, updated_at, pt 
FROM rmdc_dw.odps_online_bank_info a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 统计验证
CREATE TABLE rmdc_dw_test1.odps_statistics_verification LIKE rmdc_dw.odps_statistics_verification;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_statistics_verification PARTITION (pt)
SELECT 
id, x_date, reg_num, realname_num, contacts_list_num, zmxy_num, jxl_num, bind_card_num, alipay_num, public_funds_num, unapply_num, apply_num, apply_success_num, apply_fail_num, real_work_num, all_verif_num, some_verif_num, created_at, updated_at, pt 
FROM rmdc_dw.odps_statistics_verification;


-- 用户联系人信息
CREATE TABLE rmdc_dw_test1.odps_user_contact LIKE rmdc_dw.odps_user_contact;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_contact PARTITION (pt)
SELECT 
id, a.user_id, relation, name, mobile, source, status, updated_at, created_at, relation_spare, name_spare, mobile_spare, pt 
FROM rmdc_dw.odps_user_contact a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 用户登录地址上报
CREATE TABLE rmdc_dw_test1.odps_user_login_upload_log LIKE rmdc_dw.odps_user_login_upload_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_login_upload_log PARTITION (pt)
SELECT 
id, a.user_id, longitude, latitude, address, time, clientType, osVersion, appVersion, deviceName, created_at, appMarket, deviceId, pt 
FROM rmdc_dw.odps_user_login_upload_log a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 报表属性表
CREATE TABLE rmdc_dw_test1.odps_report_form_attribute LIKE rmdc_dw.odps_report_form_attribute;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_report_form_attribute PARTITION (pt)
SELECT 
id, statement_name, stats_date, fastest, slowest, avg, unusual, success_rate, created_at, updated_at, xdata, finished_at, pt 
FROM rmdc_dw.odps_report_form_attribute;


-- 报表响应时间临时记录表
CREATE TABLE rmdc_dw_test1.odps_report_form_response_tmp_log LIKE rmdc_dw.odps_report_form_response_tmp_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_report_form_response_tmp_log PARTITION (pt)
SELECT 
uuid, statement_name, user_name, type, response_start_time, response_end_time, created_at, updated_at, xdata, pt 
FROM rmdc_dw.odps_report_form_response_tmp_log;



-- 催收订单表
CREATE TABLE rmdc_dw_test1.odps_loan_collection_order LIKE rmdc_dw.odps_loan_collection_order;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_loan_collection_order PARTITION (pt)
SELECT 
a.id, client_id, user_id, user_loan_order_id, sync_plan_fee_time, sync_true_repayment_time, user_loan_order_repayment_id, dispatch_way, dispatch_name, dispatch_time, current_collection_admin_user_id, current_overdue_level, s1_approve_id, s2_approve_id, s3_approve_id, s4_approve_id, status, promise_repayment_time, last_collection_time, next_loan_advice, created_at, updated_at, operator_name, remark, outside_person, before_status, outside, renew_status, current_overdue_group, last_overdue_group, s5_approve_id, s6_approve_id, has_payed_money, collection_result, next_follow_time, tag, last_operate_id, last_operate_status, display_status, principal, pt 
FROM rmdc_dw.odps_loan_collection_order a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_loan_order_id = b.id;



-- 催收记录表
CREATE TABLE rmdc_dw_test1.odps_loan_collection_record_new LIKE rmdc_dw.odps_loan_collection_record_new;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_loan_collection_record_new PARTITION (pt)
SELECT 
a.id, a.order_id, operator, contact_id, contact_type, contact_name, relation, contact_phone, stress_level, order_level, order_state, operate_type, operate_source, operate_id, operate_at, content, created_at, updated_at, remark, promise_repayment_time, collection_result, next_follow_time, pt 
FROM rmdc_dw.odps_loan_collection_record_new a
JOIN (
	SELECT id FROM rmdc_dw_test1.odps_loan_collection_order
) b
ON a.order_id = b.id;


-- 用户修改信息记录
CREATE TABLE rmdc_dw_test1.odps_user_update_field_log LIKE rmdc_dw.odps_user_update_field_log;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_update_field_log PARTITION (pt)
SELECT 
id, a.user_id, field, table_name, field_before, field_after, created_at, updated_at, pt 
FROM rmdc_dw.odps_user_update_field_log a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;


-- 员工帮-银行卡管理表
CREATE TABLE rmdc_dw_test1.odps_card_info LIKE rmdc_dw.odps_card_info;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_card_info PARTITION (pt)
SELECT 
id, a.user_id, bank_id, bank_name, card_no, credit_amount, valid_period, type, phone, status, main_card, created_at, updated_at, name, bank_address, pt 
FROM rmdc_dw.odps_card_info a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;



-- 用户详情表（记录不常用的用户属性）
CREATE TABLE rmdc_dw_test1.odps_user_detail LIKE rmdc_dw.odps_user_detail;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_detail PARTITION (pt)
SELECT 
id, a.user_id, username, reg_client_type, reg_app_version, reg_device_name, reg_os_version, reg_app_market, qr_code, invite_type, exclusive_type, invite_key, intergration, phone_address, phone_carrier, user_address, self_selection, created_at, company_id, company_name, company_email, contacts_type, contacts_name, contacts_mobile, company_phone, company_address, updated_at, pt 
FROM rmdc_dw.odps_user_detail a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;



-- 用户详情表（记录不常用的用户属性）
CREATE TABLE rmdc_dw_test1.odps_user_proof_materia LIKE rmdc_dw.odps_user_proof_materia;
INSERT OVERWRITE TABLE rmdc_dw_test1.odps_user_proof_materia PARTITION (pt)
SELECT 
id, a.user_id, type, pic_name, url, created_at, updated_at, status, ocr_type, pt 
FROM rmdc_dw.odps_user_proof_materia a
JOIN (
	SELECT user_id FROM rmdc_dw_test1.odps_user_loan_order
) b
ON a.user_id = b.user_id;