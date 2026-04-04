with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i in range(1670, 1680):
    if "if group == 'an_chung':" in lines[i]:
        idx = i
        break
replacement = '''            detail_item = {
                'Ngày': r.work_date,
                'Mã NV': emp.employee_code,
                'MS': emp.masl,
                'Họ và tên': emp.full_name,
                'Vị trí': emp.position,
                'Task': r.task_id,
                'Account': r.account_id,
                'Khách hàng': r.customer_id,
                'CBM chưa hệ số': raw_cbm,
                'CBM có hệ số': converted_cbm,
            }

            if group == \\'an_chung\\':
                an_chung_detail_data.append(detail_item)
            elif group == \\'khoan\\':
                khoan_detail_data.append(detail_item)
'''
lines = lines[:idx] + [replacement] + lines[idx+13:]
with open('app.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)
