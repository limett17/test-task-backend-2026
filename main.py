import logging
from typing import Iterator, List, Dict, Tuple, Optional, Set

from sqlalchemy import select, delete
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import create_or_update_customer, \
    dict_compare_merge, create_or_update_lead, preload_customer_indexes, \
    preload_lead_indexes
from amocrm import Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from models import db, AbstractCustomer

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
CLEANUP_CHUNK = 500


def _load_batch(existing: Dict[int, AbstractCustomer], needed_ids: Set[int]) -> None:
    """Подгружает одним SQL недостающие объекты в `existing`."""
    missing = [i for i in needed_ids if i is not None and i not in existing]
    if not missing:
        return
    for row in db.session.scalars(
        select(AbstractCustomer).where(AbstractCustomer.id.in_(missing))
    ):
        existing[row.id] = row


def _track_entry(ids_map: Dict[int, dict], abc_id: int,
                 *, customers=None, leads=None, contacts=None, companies=None) -> None:
    """Безопасно добавляет данные в ids_map[abc_id]."""
    entry = ids_map.setdefault(abc_id, {
        "customers": [], "leads": [], "contacts": [], "companies": [],
    })
    if customers:
        entry["customers"].extend(customers)
    if leads:
        entry["leads"].extend(leads)
    if contacts:
        entry["contacts"].extend(contacts)
    if companies:
        entry["companies"].extend(companies)


def _redirect_indexes(old_id: int, new_id: int,
                      cust_by_id: Dict[int, int],
                      cust_by_login: Dict[str, List[int]],
                      lead_by_id: Dict[int, int],
                      lead_by_login: Dict[str, List[int]]) -> None:
    """После merge переносит все ссылки old_id → new_id."""
    for d in (cust_by_id, lead_by_id):
        for k, v in list(d.items()):
            if v == old_id:
                d[k] = new_id
    for d in (cust_by_login, lead_by_login):
        for k, lst in d.items():
            d[k] = [new_id if x == old_id else x for x in lst]


def lead_action(lead: Lead,
                existing: Dict[int, AbstractCustomer],
                cust_by_id: Dict[int, int],
                cust_by_login: Dict[str, List[int]],
                lead_by_id: Dict[int, int],
                lead_by_login: Dict[str, List[int]],
                ) -> Tuple[AbstractCustomer, bool]:
    abc_id_by_lead = lead_by_id.get(lead.id)

    lead_login = lead.get_field('Логин')
    abc_id_by_login: Optional[int] = None
    if lead_login:
        candidates = cust_by_login.get(lead_login, [])
        if len(candidates) == 1:
            abc_id_by_login = candidates[0]

    abc_by_lead = existing.get(abc_id_by_lead) if abc_id_by_lead else None
    abc_by_login = existing.get(abc_id_by_login) if abc_id_by_login else None

    if abc_by_lead is None:
        abc = abc_by_login
    elif abc_by_login is not None and abc_by_login.id != abc_by_lead.id:
        abc_by_lead.amo_customers = dict_compare_merge(
            abc_by_lead.amo_customers or [], abc_by_login.amo_customers or [])
        abc_by_lead.amo_leads = dict_compare_merge(
            abc_by_lead.amo_leads or [], abc_by_login.amo_leads or [])
        abc_by_lead.amo_contacts = dict_compare_merge(
            abc_by_lead.amo_contacts or [], abc_by_login.amo_contacts or [])
        abc_by_lead.amo_companies = dict_compare_merge(
            abc_by_lead.amo_companies or [], abc_by_login.amo_companies or [])
        flag_modified(abc_by_lead, "amo_customers")
        flag_modified(abc_by_lead, "amo_leads")
        flag_modified(abc_by_lead, "amo_contacts")
        flag_modified(abc_by_lead, "amo_companies")
        db.session.delete(abc_by_login)
        existing.pop(abc_by_login.id, None)
        _redirect_indexes(abc_by_login.id, abc_by_lead.id,
                          cust_by_id, cust_by_login, lead_by_id, lead_by_login)
        abc = abc_by_lead
    else:
        abc = abc_by_lead

    is_new = abc is None
    abc = create_or_update_lead(abc, lead)
    return abc, is_new


def collect_leads_customers(is_full=True):
    """
     Берем всех покупателей, если его нет в бд - делаем новую запись AbstractCutomer, в него кладем.
     Есть есть - находим его запись по id и перезаписываем

     Вот так у нас коллектятся все кастомеры

     Далее берутся все лиды. Ищутся их записи в бд по id, если не находится и есть логин
     - находим его запись по кастомеру с тем же логином, кладем к нему.
     Если нет логина/нет кастомера - пропускаем. Если нашелся - перезаписываем соответственно
         abstract_id_customer : {
        customers: [...],
        leads: [...],
        contacts: [...],
        companies: [...]
    }
    """
    logger.info(f"Collecting customers and leads {is_full=}")

    cust_by_id, cust_by_login = preload_customer_indexes()
    lead_by_id, lead_by_login = preload_lead_indexes()

    ids_map: Dict[int, dict] = {}

    customers: Iterator[Customer] = amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}")
    batch: List[Customer] = []
    existing_c: Dict[int, AbstractCustomer] = {}

    def flush_customer_batch():
        needed: Set[int] = set()
        for c in batch:
            abc_id = cust_by_id.get(c.id)
            if abc_id is None:
                candidates = lead_by_login.get(c.get_field('Логин') or '', [])
                if len(candidates) == 1:
                    abc_id = candidates[0]
            if abc_id:
                needed.add(abc_id)
        _load_batch(existing_c, needed)

        processed = []
        for c in batch:
            abc_id = cust_by_id.get(c.id)
            if abc_id is None:
                candidates = lead_by_login.get(c.get_field('Логин') or '', [])
                if len(candidates) == 1:
                    abc_id = candidates[0]
            abc = existing_c.get(abc_id) if abc_id else None
            is_new = abc is None
            abc = create_or_update_customer(abc, c)
            processed.append((abc, c, is_new))

        db.session.flush()

        for abc, c, is_new in processed:
            login = c.get_field('Логин') or ''
            if is_new:
                cust_by_id[c.id] = abc.id
                if abc.id not in cust_by_login[login]:
                    cust_by_login[login].append(abc.id)
            if is_full:
                _track_entry(ids_map, abc.id,
                             customers=[c.id],
                             contacts=[co["id"] for co in c.contacts],
                             companies=[co["id"] for co in c.companies])

    for customer in customers:
        if is_full is False and customer.status_id == 111349:
            continue
        if not customer.get_field('Логин'):
            continue
        batch.append(customer)
        if len(batch) >= BATCH_SIZE:
            flush_customer_batch()
            db.session.commit()
            db.session.expunge_all()
            existing_c = {}
            batch = []
    if batch:
        flush_customer_batch()
    db.session.commit()
    db.session.expunge_all()
    del customers

    amo_leads: Iterator[Lead] = amocrm_get(Lead, _with=f"{CONTACTS}")
    batch = []
    existing_l: Dict[int, AbstractCustomer] = {}

    def flush_lead_batch():
        needed: Set[int] = set()
        for lead in batch:
            abc_id = lead_by_id.get(lead.id)
            if abc_id:
                needed.add(abc_id)
            login = lead.get_field('Логин')
            if login:
                candidates = cust_by_login.get(login, [])
                if len(candidates) == 1:
                    needed.add(candidates[0])
        _load_batch(existing_l, needed)

        processed = []
        for lead in batch:
            try:
                abc, is_new = lead_action(
                    lead, existing_l,
                    cust_by_id, cust_by_login, lead_by_id, lead_by_login,
                )
                processed.append((abc, lead, is_new))
            except Exception as e:
                logger.exception(f'{lead} {e}')

        db.session.flush()

        for abc, lead, is_new in processed:
            login = lead.get_field('Логин')
            if is_new:
                lead_by_id[lead.id] = abc.id
                if login and abc.id not in lead_by_login[login]:
                    lead_by_login[login].append(abc.id)
                existing_l[abc.id] = abc
            if is_full:
                _track_entry(ids_map, abc.id,
                             leads=[lead.id],
                             contacts=[co["id"] for co in lead.contacts])

    for amo_lead in amo_leads:
        if is_full is False and amo_lead.status_id == 143:
            continue
        batch.append(amo_lead)
        if len(batch) >= BATCH_SIZE:
            flush_lead_batch()
            db.session.commit()
            db.session.expunge_all()
            existing_l = {}
            batch = []
    if batch:
        flush_lead_batch()
    db.session.commit()
    db.session.expunge_all()

    if is_full is False:
        return

    try:
        logger.info("Чистка покупателей общая")
        if not ids_map:
            db.session.execute(delete(AbstractCustomer))
        else:
            db.session.execute(
                delete(AbstractCustomer).where(
                    AbstractCustomer.id.not_in(list(ids_map.keys()))
                )
            )
        db.session.commit()

        logger.info("Чистка покупателей на уровне сущностей")
        ids = list(ids_map.keys())
        for i in range(0, len(ids), CLEANUP_CHUNK):
            chunk = ids[i:i + CLEANUP_CHUNK]
            for abc_customer in db.session.scalars(
                    select(AbstractCustomer).where(AbstractCustomer.id.in_(chunk))
            ):
                filter_abc_customer(abc_customer, ids_map)
            db.session.commit()
            db.session.expunge_all()
    except Exception as e:
        logger.exception(f"Чистка: {e}")


def filter_merge_abstract_customer(abc_customer: AbstractCustomer, attr: str,
                                   check_ids: List[int]):
    entity = getattr(abc_customer, attr) or []
    check_set = set(check_ids)
    filtered = [
        item for item in entity
        if 'удалить' not in ((item.get('general', {}).get('name') or '').lower())
           and item.get('general', {}).get('id') in check_set
    ]
    if len(filtered) != len(entity):
        setattr(abc_customer, attr, filtered)


def filter_abc_customer(abc_customer: AbstractCustomer, ids_map: Dict):
    entry = ids_map.get(abc_customer.id)
    if entry is None:
        return

    if abc_customer.amo_leads:
        try:
            filter_merge_abstract_customer(abc_customer, "amo_leads", entry["leads"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_customers:
        try:
            filter_merge_abstract_customer(abc_customer, "amo_customers", entry["customers"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_contacts:
        try:
            filter_merge_abstract_customer(abc_customer, "amo_contacts", entry["contacts"])
        except Exception as e:
            logger.exception(e)
    if abc_customer.amo_companies:
        try:
            filter_merge_abstract_customer(abc_customer, "amo_companies", entry["companies"])
        except Exception as e:
            logger.exception(e)
    if not abc_customer.amo_leads and not abc_customer.amo_customers:
        db.session.delete(abc_customer)
