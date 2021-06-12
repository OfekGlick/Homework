import os
import csv
import pyodbc
from os import path as pth
from multiprocessing import Process
from collections import deque
import time

X = 29
X_yoni_yosi_eyal = 32
Y = 11
Z = 59
PREFORM_STEP_ONE = 1
PREFORM_STEP_TWO = 2
PREFORM_STEP_THREE = 3


class InventoryError(Exception):
    """Error if there are not enough items in the inventory"""
    pass


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
            if lock[0] == transactionID and lock[1] == productID and lock[2] == lock_type:
                print("no lock needed")
                return True
        give_lock = f"""insert into Locks (transactionID, productID, lockType) values
                        (?,?,?)"""
        give_lock_log = f"""insert into Locks (transactionID, productID, lockType) values
                        ({transactionID},{productID},{lock_type})"""
        if len(other_locks) == 0:
            update_log(cursor, transactionID, 'Locks', productID, 'insert', give_lock_log)
            cursor.execute(give_lock, (transactionID, productID, lock_type))
            cursor.commit()
            return True
        else:
            if lock_type == 'write':
                return False
            else:
                other_locks = list(zip(*other_locks))[2]
                if 'write' in other_locks:
                    return False
                else:
                    update_log(cursor, transactionID, 'Locks', productID, 'insert', give_lock_log)
                    cursor.execute(give_lock, (transactionID, productID, lock_type))
                    cursor.commit()
                    return True
    else:
        check = f"""SELECT * FROM Locks WHERE transactionID='{transactionID}'
                                                and productID={productID}
                                                and lockType='{lock_type}'
        """
        if len(cursor.execute(check).fetchall()) != 0:
            release_lock = f"""DELETE FROM Locks WHERE transactionID='{transactionID}'
                                                    and productID={productID}
                                                    and lockType='{lock_type}'"""
            update_log(cursor, transactionID, 'Locks', productID, 'delete', release_lock)
            cursor.execute(release_lock)
            cursor.commit()


def update_inventory(cursor, transactionID):
    get_inv = "select * from ProductsInventory"
    #     REQUEST READ LOCK!!!!!!!!!!!!!!!!!!!
    cursor.execute(get_inv)
    #     RELEASE READ LOCK!!!!!!!!!!!!!!!!!!!
    data = cursor.fetchall()
    inven = [60 if i == 1 else 53 for i in range(1, Y + 1)]
    #     REQUEST WRITE LOCK!!!!!!!!!!!!!!!!!!!
    if len(data) == 0:
        for i, amount in enumerate(inven):
            query = f"""insert into ProductsInventory (productID, inventory) values
                        ({i + 1},{amount})"""
            cursor.execute(query)
    else:
        for i, amount in enumerate(inven):
            query = f"""update ProductsInventory 
                        set inventory = {amount}
                        where productID = {i + 1}"""
            cursor.execute(query)
    cursor.commit()


#     RELEASE WRITE LOCK!!!!!!!!!!!!!!!!!!!




def execute_query_on_site(cursor, transactionID, order, step=1):
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
                    raise InventoryError
            step = PREFORM_STEP_TWO
        else:
            return None, PREFORM_STEP_ONE
    if step == PREFORM_STEP_TWO:
        for productID, _ in order:
            lock_management(cursor, transactionID, productID, 'release', 'read')
            if lock_management(cursor, transactionID, productID, 'acquire', 'write'):
                continue
            else:
                return None, PREFORM_STEP_TWO
        step = PREFORM_STEP_THREE
    if step == PREFORM_STEP_THREE:
        for (productID, amount), in_stock in zip(order, inventory):
            query = f"update ProductsInventory set inventory = {int(in_stock) - int(amount)} where productID = {productID}"
            update_log(cursor, transactionID, 'ProductsInventory', productID, 'update', query, False)
            cursor.execute(query)
        return cursor, -1


def manage_transaction(transactionID, order, data):
    relevant = deque([(site, uid, 1) for site, uid in data if str(site) in order.keys()])
    cursor_lst = []
    while len(relevant) > 0:
        order_queue = list(relevant)
        for (site, uid, step) in order_queue:
            site_cur = DBconnect(uid)
            site_order = order[str(site)]
            try:
                cursor, status = execute_query_on_site(site_cur, transactionID, site_order, step)
                if status != -1:
                    relevant.popleft()
                    relevant.append((site, uid, status))
                else:
                    relevant.popleft()
                    cursor_lst.append((cursor, site_order))
            except InventoryError:
                return None

    for cursor, site_order in cursor_lst:
        [lock_management(cursor, transactionID, productID, 'release', 'write') for productID, _ in site_order]


#   UPDATE PRODUCTS_ORDERED


# This needs to happen after all sites have finished


def divide_to_sites(order):
    new_order = {key: [] for key, _, _ in order}
    for req in order:
        new_order[req[0]].append((req[1], req[2]))
    return new_order


def manage_transactions(T):
    bigserver_cursor = DBconnect('dbteam')
    bigserver_cursor.execute("select * from categoriestosites")
    data = bigserver_cursor.fetchall()
    path = r"C:\Users\Ofek\PycharmProjects\GitRepo\DDBS\orders"
    for order in sorted(os.listdir(path)):
        with open(pth.join(path, order), encoding='utf-8-sig') as curr_order:
            transactionID = order[:-4] + "_" + str(X)
            order = divide_to_sites(list(csv.reader(curr_order)))
            transaction_proc = Process(target=manage_transaction, args=(transactionID, order, data))
            transaction_proc.start()
            transaction_proc.join(timeout=T)
            if transaction_proc.is_alive():
                print(transactionID + "failed")
                transaction_proc.terminate()
            else:
                print("transaction finished")

#                 preform Undo on TransactionID


if __name__ == '__main__':
    cur = DBconnect("ofek0glick")
    drop_tables(cur)
    create_tables(cur)
    update_inventory(cur, "bla")
    manage_transactions(35)
