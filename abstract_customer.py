import json
import logging
from collections import defaultdict
from typing import Optional, Dict, Tuple, List

from sqlalchemy import text

from amocrm import Customer, Lead
from models import AbstractCustomer, db, AbcCustomerEntities

logger = logging.getLogger(__name__)


def update_jsonb_field(entity_id: int,
                       attribute: AbcCustomerEntities,
                       index: int, new_value: Dict,
                       table_name='abstract_customer'):
    """Обновление jsonb поля в указанной таблице.
    :param table_name: Название таблицы (должно быть прописано).
    :param entity_id: ид абстрактного покупателя.
    :param attribute: Поле таблицы, которое надо обновить в формате Enum.
    :param index: Индекс изменения - цифра если массив, массив строк если словарь. Цифра должна быть точной.
    :param new_value: Новые данные для изменения
    """
    if table_name != 'abstract_customer':
        raise Exception("Неизвестная таблица")
    query = text(f"""
        UPDATE {table_name}
        SET {attribute.value} = jsonb_set({attribute.value}, :path, :value)
        WHERE id = :entity_id
    """)
    db.session.execute(query, {'path': f'{{{index}}}', 'value': json.dumps(new_value), 'entity_id': entity_id})


def dict_compare_merge(l_dict1, l_dict2):
    d = defaultdict(dict)
    for i in l_dict1 + l_dict2:
        d[i['general']['id']].update(i)

    return list(d.values())


def get_customer_json(customer: Customer) -> dict:
    return {
        'general': {
            'id': customer.id,
            'login': customer.custom_fields_values.get('Логин', {}).get('value'),
            'billing_type': customer.custom_fields_values.get('Вид оплаты', {}).get('value'),
            'is_robot': bool(customer.custom_fields_values.get('Робот')),
            'is_nds': bool(customer.custom_fields_values.get('Оплата с НДС')),
            'system': customer.custom_fields_values.get('Система', {}).get('value'),
            'host': customer.custom_fields_values.get('Хост', {}).get('value'),
            'name': customer.name,
        },
        'api_value': customer.as_dict()
    }


def get_lead_json(lead: Lead) -> dict:
    return {
        'general': {
            'id': lead.id,
            'login': lead.custom_fields_values.get('Логин', {}).get('value'),
            'name': lead.name,
        },
        'api_value': lead.as_dict()
    }


def get_abccustomer_by_lead_login(lead_login: str) -> Optional[AbstractCustomer]:
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_leads.op('@>')([{'general': {'login': lead_login}}])).all()

    if len(customers) > 1:
        logger.warning(f"Lead {lead_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def get_abccustomer_by_amocustomer_login(customer_login: str) -> Optional[AbstractCustomer]:
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_customers.op('@>')([{'general': {'login': customer_login}}])).all()

    if len(customers) > 1:
        logger.warning(f"Customer {customer_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def preload_customer_indexes() -> Tuple[Dict[int, int], Dict[str, List[int]]]:
    by_crm_id: Dict[int, int] = {}
    by_login: Dict[str, List[int]] = defaultdict(list)
    stmt = text("""
        SELECT ac.id AS abc_id,
               (elem->'general'->>'id')::bigint AS crm_id,
               elem->'general'->>'login' AS login
        FROM abstract_customer ac,
             jsonb_array_elements(ac.amo_customers) AS elem
        WHERE ac.amo_customers IS NOT NULL
          AND jsonb_typeof(ac.amo_customers) = 'array'
    """)
    for row in db.session.execute(stmt):
        if row.crm_id is not None:
            by_crm_id[row.crm_id] = row.abc_id
        if row.login:
            by_login[row.login].append(row.abc_id)
    return by_crm_id, by_login


def preload_lead_indexes() -> Tuple[Dict[int, int], Dict[str, List[int]]]:
    by_crm_id: Dict[int, int] = {}
    by_login: Dict[str, List[int]] = defaultdict(list)
    stmt = text("""
        SELECT ac.id AS abc_id,
               (elem->'general'->>'id')::bigint AS crm_id,
               elem->'general'->>'login' AS login
        FROM abstract_customer ac,
             jsonb_array_elements(ac.amo_leads) AS elem
        WHERE ac.amo_leads IS NOT NULL
          AND jsonb_typeof(ac.amo_leads) = 'array'
    """)
    for row in db.session.execute(stmt):
        if row.crm_id is not None:
            by_crm_id[row.crm_id] = row.abc_id
        if row.login:
            by_login[row.login].append(row.abc_id)
    return by_crm_id, by_login


def create_or_update_customer(abstract_customer: Optional[AbstractCustomer],
                              amo_customer: Customer) -> AbstractCustomer:
    new_json = get_customer_json(amo_customer)
    if abstract_customer is None:
        obj = AbstractCustomer(
            amo_customers=[new_json],
        )
        db.session.add(obj)
        return obj

    current = list(abstract_customer.amo_customers or [])
    replaced = False
    for i, item in enumerate(current):
        if (item.get('general') or {}).get('id') == amo_customer.id:
            current[i] = new_json
            replaced = True
            break
    if not replaced:
        current.append(new_json)
    abstract_customer.amo_customers = current
    return abstract_customer


def create_or_update_lead(abc_customer: Optional[AbstractCustomer], amo_lead: Lead) -> AbstractCustomer:
    new_json = get_lead_json(amo_lead)
    if abc_customer is None:
        obj = AbstractCustomer(amo_leads=[new_json])
        db.session.add(obj)
        return obj

    current = list(abc_customer.amo_leads or [])
    replaced = False
    for i, item in enumerate(current):
        if (item.get('general') or {}).get('id') == amo_lead.id:
            current[i] = new_json
            replaced = True
            break
    if not replaced:
        current.append(new_json)
    abc_customer.amo_leads = current
    return abc_customer
