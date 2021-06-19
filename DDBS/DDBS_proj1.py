import multiprocessing
import os
import csv
import pyodbc
from os import path as pth
from multiprocessing import Process, Queue
from collections import deque
from time import time
from tqdm import tqdm

X = 29
X_yoni_yosi_eyal = 32
Y = 11
Z = 59
PREFORM_STEP_ONE = 1
PREFORM_STEP_TWO = 2
PREFORM_STEP_THREE = 3


def DBconnect(username):
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=technionddscourse.database.windows.net;'
                          f'DATABASE={username};'
                          f'UID={username};'
                          'PWD=Qwerty12!')
    cursor = conn.cursor()
    return cursor


def drop_tables(cursor):
    try:
        cursor.execute("drop table Locks")
        cursor.execute("drop table Log")
        cursor.execute("drop table ProductsOrdered")
        cursor.execute("drop table ProductsInventory")
        cursor.commit()
        print("done")
    except:
        print("nothing to delete")


def create_tables(cursor):
    cursor.execute("""CREATE TABLE ProductsInventory (
        productID integer primary key,
        inventory integer ,
        check (inventory >= 0)); """)
    cursor.execute("""CREATE TABLE ProductsOrdered (
        transactionID varchar(30) ,
        productID integer ,
        amount integer,
        check (amount >= 1),
        foreign key(productID) references ProductsInventory(productID)
        on delete cascade,
        primary key(transactionID, productID)
        ); """)
    cursor.execute("""CREATE TABLE Log (
        rowID integer IDENTITY(1,1) primary key,
        timestamp datetime ,
        relation varchar(30),
        transactionID varchar(30),
        productID integer,
        action varchar(20),
        record varchar(2500),
        CHECK(relation IN ('ProductsInventory', 'ProductsOrdered','Locks')),
        CHECK(action IN ('read', 'update','insert','delete')),
        foreign key(productID) references ProductsInventory(productID)
        on delete cascade); """)
    cursor.execute("""CREATE TABLE Locks (
        transactionID varchar(30),
        productID integer ,
        lockType varchar(10),
        CHECK(lockType IN ('read', 'write')),
        foreign key(productID) references ProductsInventory(productID)
        on delete cascade,
        primary key(transactionID,productID,lockType) 
        ); """)
    cursor.commit()


def update_log(cursor, transactionID, relation, productID, action, record, commit=True):
    cursor.execute(f"""insert into Log (timestamp,relation,transactionID,productID,action,record) values
                   (CURRENT_TIMESTAMP,?,?,?,?,?)""", (relation, transactionID, productID, action, record))
    if commit:
        cursor.commit()


def lock_management(cursor, transactionID, productID, action, lock_type):
    if action == 'acquire':
        get_locks = f"""select * from Locks where productID = {productID}"""
        update_log(cursor, transactionID, 'Locks', productID, 'read', get_locks)
        locks = cursor.execute(get_locks).fetchall()
        other_locks = [(tran, prod, typ) for tran, prod, typ in locks if tran != transactionID]
        my_locks = [(tran, prod, typ) for tran, prod, typ in locks if tran == transactionID]
        for lock in my_locks:
            if lock[0] == transactionID and str(lock[1]) == str(productID) and lock[2] == lock_type:
                return True
        give_lock = f"""insert into Locks (transactionID, productID, lockType) values
                        (?,?,?)"""
        give_lock_log = f"""insert into Locks (transactionID, productID, lockType) values
                        ({transactionID},{productID},{lock_type})"""
        if len(other_locks) == 0:
            try:
                update_log(cursor, transactionID, 'Locks', productID, 'insert', give_lock_log, False)
                cursor.execute(give_lock, (transactionID, productID, lock_type))
                cursor.commit()
                return True
            except pyodbc.IntegrityError:
                return False
        else:
            if lock_type == 'write':
                return False
            else:
                other_locks = list(zip(*other_locks))[2]
                if 'write' in other_locks:
                    return False
                else:
                    try:
                        update_log(cursor, transactionID, 'Locks', productID, 'insert', give_lock_log, False)
                        cursor.execute(give_lock, (transactionID, productID, lock_type))
                        cursor.commit()
                        return True
                    except pyodbc.IntegrityError:
                        return False
    else:
        check = f"""SELECT * FROM Locks WHERE transactionID='{transactionID}'
                                                and productID={productID}
                                                and lockType='{lock_type}'
        """
        if len(cursor.execute(check).fetchall()) != 0:
            release_lock = f"""DELETE FROM Locks WHERE transactionID='{transactionID}'
                                                    and productID={productID}
                                                    and lockType='{lock_type}'"""
            update_log(cursor, transactionID, 'Locks', productID, 'delete', release_lock, False)
            cursor.execute(release_lock)
            cursor.commit()


def update_inventory(transactionID):
    cursor = DBconnect("ofek0glick")
    inven = [60 if i == 1 else 53 for i in range(1, Y + 1)]
    try:
        can_update = all(
            [lock_management(cursor, transactionID, productID + 1, 'acquire', 'write') for productID, amount in
             enumerate(inven)])
        from time import time
        start = time()
        print("trying to restock")
        while not can_update:
            if time() - start >= 120:
                break
            can_update = all(
                [lock_management(cursor, transactionID, productID + 1, 'acquire', 'write') for productID, amount in
                 enumerate(inven)])
        if can_update:
            query = """update ProductsInventory 
                        set inventory = {0}
                        where productID = {1}"""
            for i, amount in enumerate(inven):
                update_log(cursor, transactionID, 'ProductsInventory', i + 1, 'update', query.format(amount, i + 1),
                           False)
                cursor.execute(query.format(amount, i + 1))
            cursor.commit()
        else:
            return
    except pyodbc.IntegrityError:
        for i, amount in enumerate(inven):
            query = f"""insert into ProductsInventory (productID, inventory) values
                        ({i + 1},{amount})"""
            cursor.execute(query).commit()
            update_log(cursor, transactionID, 'ProductsInventory', i + 1, 'insert', query)
    [lock_management(cursor, transactionID, i, 'release', 'write') for i in range(1, 12)]
    print("Stocked up")


def execute_query_on_site(cursor, transactionID, order, inventory, step):
    if step == PREFORM_STEP_ONE:
        can_continue = all(
            [lock_management(cursor, transactionID, productID, 'acquire', 'read') for productID, _ in order])
        if can_continue:
            inventory = []
            for productID, _ in order:
                query = "select inventory from ProductsInventory WHERE productID = ?"
                query_log = "select inventory from ProductsInventory WHERE productID = {productID}"
                update_log(cursor, transactionID, "ProductsInventory", productID, 'read', query_log)
                inventory.append(cursor.execute(query, productID).fetchall()[0][0])

            for needed, in_stock in zip([x[1] for x in order], inventory):
                if in_stock - int(needed) < 0:
                    return None, -2, []
            step = PREFORM_STEP_TWO
        else:
            return None, PREFORM_STEP_ONE, inventory
    if step == PREFORM_STEP_TWO:
        for productID, _ in order:
            lock_management(cursor, transactionID, productID, 'release', 'read')
            if lock_management(cursor, transactionID, productID, 'acquire', 'write'):
                continue
            else:
                return None, PREFORM_STEP_TWO, inventory
        step = PREFORM_STEP_THREE
    if step == PREFORM_STEP_THREE:
        for (productID, amount), in_stock in zip(order, inventory):
            query = f"update ProductsInventory set inventory = {int(in_stock) - int(amount)} where productID = " \
                    f"{productID}"
            update_log(cursor, transactionID, 'ProductsInventory', productID, 'update', query, False)
            cursor.execute(query)
        prod_id_lst = list(zip(list(zip(*order))[0], inventory))
        return cursor, -1, prod_id_lst


def update_productOrder(cursor, transactionID, site_order):
    sqlquery = """insert into ProductsOrdered(transactionID, productID, amount) values
                    (?,?,?)"""
    sqlquerylog = """insert into ProductsOrdered(transactionID, productID, amount) values
                        ('{0}',{1},{2})"""
    for productID, amount in site_order:
        update_log(cursor, transactionID, 'ProductsOrdered', productID, 'insert',
                   sqlquerylog.format(transactionID, productID, amount), False)
        cursor.execute(sqlquery, (transactionID, productID, amount))


def manage_transaction(transactionID, order, data, return_dict):
    relevant = deque([(site, uid, 1) for site, uid in data if str(site) in order.keys()])
    cursor_lst = []
    inventory = {site: [] for site in order.keys()}
    flag = True
    while len(relevant) > 0:
        order_queue = list(relevant)
        for (site, uid, step) in order_queue:
            site_cur = DBconnect(uid)
            site_order = order[str(site)]
            cursor, step, site_inventory = execute_query_on_site(
                site_cur, transactionID, site_order, inventory[str(site)], step)
            if step == -2:
                return_dict[-1] = False
                return
            inventory[str(site)] = site_inventory
            if step != -1:
                relevant.popleft()
                relevant.append((site, uid, step))
            else:
                relevant.popleft()
                cursor_lst.append((cursor, site, site_order))
        if not flag:
            break

    return_dict["inventory"] = inventory

    return_dict[0] = False

    for cursor, site, site_order in cursor_lst:
        update_productOrder(cursor, transactionID, site_order)

    for cursor, _, _ in cursor_lst:
        cursor.commit()

    return_dict[1] = False

    for cursor, _, site_order in cursor_lst:
        [lock_management(cursor, transactionID, productID, 'release', 'write') for productID, _ in site_order]

    return_dict[2] = False


def undo(transactionID, prev_inventory: dict, order_data, site_to_uid):
    print("preforming Undo")
    site_cursors = {str(site): DBconnect(uid) for site, uid in site_to_uid if str(site) in order_data.keys()}
    find_locks = """select productID from  locks where transactionID = ?"""
    find_locks_log = """select productID from  locks where transactionID = '{0}'"""
    release_lock = """DELETE FROM Locks WHERE transactionID = ?"""
    release_lock_log = """DELETE FROM Locks WHERE transactionID='{0}'"""
    delete_inventory = """DELETE FROM ProductsOrdered WHERE transactionID = ?"""
    delete_inventory_log = """DELETE FROM ProductsOrdered WHERE transactionID = '{0}'"""
    correct_inventory = """update ProductsInventory set inventory = ? where productID = ?"""
    correct_inventory_log = """update ProductsInventory set inventory = {0} where productID = {1}"""
    if len(prev_inventory.keys()) == 0:
        for site in order_data.keys():
            site_cur = site_cursors[site]
            result = site_cur.execute(find_locks, transactionID).fetchall()
            for productID in result:
                update_log(site_cur, transactionID, 'Locks', productID[0], 'read', find_locks_log.format(transactionID))
            for productID in result:
                update_log(site_cur, transactionID, 'Locks', productID[0], 'delete',
                           release_lock_log.format(transactionID, productID[0]))
            site_cur.execute(release_lock, transactionID)
            site_cur.commit()
    else:
        for site in prev_inventory.keys():
            site_cur = site_cursors[site]
            # reset inventory
            for productID, amount in prev_inventory[site]:
                update_log(site_cur, transactionID, 'ProductsInventory', productID, 'update',
                           correct_inventory_log.format(transactionID, productID))
                site_cur.execute(correct_inventory, amount, productID)
            # delete product ordered
            for productID, _ in prev_inventory[site]:
                update_log(site_cur, transactionID, 'ProductsOrdered', productID, 'delete',
                           delete_inventory_log.format(transactionID))
            site_cur.execute(delete_inventory, transactionID)
            # release locks
            for productID, _ in prev_inventory[site]:
                update_log(site_cur, transactionID, 'Locks', productID, 'delete',
                           release_lock_log.format(transactionID))
                site_cur.execute(release_lock, transactionID)
                site_cur.commit()


def divide_to_sites(order):
    order = order[1:]
    new_order = {key: [] for key, _, _ in order}
    for req in order:
        new_order[req[0]].append((req[1], req[2]))
    return new_order


def manage_transactions(T):
    bigserver_cursor = DBconnect('dbteam')
    bigserver_cursor.execute("select * from categoriestosites")
    data = bigserver_cursor.fetchall()
    path = r"C:\Users\Ofek\PycharmProjects\GitRepo\DDBS\orders"
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    successful_transactions = []
    failed_transactions = []
    for order in sorted(os.listdir(path)):
        with open(pth.join(path, order), encoding='utf-8-sig') as curr_order:
            transactionID = order[:-4] + "_" + str(X)
            print(transactionID)
            order_data = divide_to_sites(list(csv.reader(curr_order)))
            for i in range(5):
                return_dict[-1], return_dict[0], return_dict[1], return_dict[2] = True, True, True, True
                transaction_proc = Process(target=manage_transaction, args=(transactionID, order_data, data, return_dict))
                start = time()
                transaction_proc.start()
                transaction_proc.join(timeout=T)
                if transaction_proc.is_alive():
                    transaction_proc.terminate()
                    print(f"timed out after {time() - start} seconds")
                    if return_dict[0]:
                        print(transactionID + " timed out")
                        undo(transactionID, {}, order_data, data)
                        failed_transactions.append(transactionID)
                    elif not return_dict[0] and return_dict[1]:
                        print(transactionID + " timed out")
                        undo(transactionID, return_dict["inventory"], order_data, data)
                        failed_transactions.append(transactionID)
                    elif not return_dict[1] and return_dict[2]:
                        site_cursors = {str(site): DBconnect(uid) for site, uid in data if
                                        str(site) in order_data.keys()}
                        for site in order_data.keys():
                            sit_cur = site_cursors[site]
                            for productID, _ in order_data[site]:
                                lock_management(sit_cur, transactionID, productID, 'release', 'write')
                            successful_transactions.append(transactionID)
                        print(transactionID + " timed out but passed")
                        break

                else:
                    if return_dict[-1]:
                        print(f"passed after {time() - start} seconds")
                        successful_transactions.append(transactionID)
                        break
                    else:
                        print(transactionID + " failed")
                        undo(transactionID, {}, order_data, data)
                        failed_transactions.append(transactionID)
                        break
            return_dict.clear()


if __name__ == '__main__':
    cur = DBconnect("ofek0glick")
    drop_tables(cur)
    create_tables(cur)
    update_inventory("Updating Inventory")
#     manage_transactions(30)
#     update_inventory("Updating Inventory")
# #