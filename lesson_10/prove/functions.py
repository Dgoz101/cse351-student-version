"""
Course: CSE 351, week 10
File: functions.py
Author: <your name>

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04

--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1
I parallelized the DFS so that fetching each husband, wife, and each child happens in its own thread. 
As soon as I get a person’s data, I use a recursive DFS for their parent and family.

Describe how to speed up part 2
I use a pool of worker threads consuming from a shared family queue.Each worker fetches a family, 
adds it if unseen, then fetches all its people in order. When a person’s parent‐family appears, 
it’s enqueued. Multiple workers will allow me to complete this for multiple families at once.

Extra (Optional) 10% Bonus to speed up part 3
Wrap the same worker/pool approach with a threading.Semaphore(5) around 
eachget_data_from_server to limit the calls to 5.
"""
from common import *
import queue
import threading

NUM_WORKERS = 100

# -----------------------------------------------------------------------------
def depth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement Depth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    def dfs_family(fam_id):
        fam_data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
        if not fam_data:
            return

        fam = Family(fam_data)
        if not tree.does_family_exist(fam.get_id()):
            tree.add_family(fam)

        threads = []

        # process husband
        hid = fam_data.get('husband_id')
        if hid is not None:
            def handle_husband():
                p_data = get_data_from_server(f"{TOP_API_URL}/person/{hid}")
                if not p_data:
                    return
                person = Person(p_data)
                if not tree.does_person_exist(person.get_id()):
                    tree.add_person(person)
                parent_fam = p_data.get('parent_id')
                if parent_fam and not tree.does_family_exist(parent_fam):
                    dfs_family(parent_fam)
            t = threading.Thread(target=handle_husband)
            t.start()
            threads.append(t)

        # process wife
        wid = fam_data.get('wife_id')
        if wid is not None:
            def handle_wife():
                p_data = get_data_from_server(f"{TOP_API_URL}/person/{wid}")
                if not p_data:
                    return
                person = Person(p_data)
                if not tree.does_person_exist(person.get_id()):
                    tree.add_person(person)
                parent_fam = p_data.get('parent_id')
                if parent_fam and not tree.does_family_exist(parent_fam):
                    dfs_family(parent_fam)
            t = threading.Thread(target=handle_wife)
            t.start()
            threads.append(t)

        # process children
        for cid in fam_data.get('children', []):
            def handle_child(child_id=cid):
                p_data = get_data_from_server(f"{TOP_API_URL}/person/{child_id}")
                if not p_data:
                    return
                child = Person(p_data)
                if not tree.does_person_exist(child.get_id()):
                    tree.add_person(child)
            t = threading.Thread(target=handle_child)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    dfs_family(family_id)


# -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    fam_queue = queue.Queue()
    fam_queue.put(family_id)

    tree_lock = threading.Lock()

    def worker():
        while True:
            fam_id = fam_queue.get()
            if fam_id is None:
                fam_queue.task_done()
                break

            fam_data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
            if fam_data:
                fam = Family(fam_data)
                with tree_lock:
                    if not tree.does_family_exist(fam.get_id()):
                        tree.add_family(fam)

                for pid in (fam_data.get('husband_id'), fam_data.get('wife_id')) + tuple(fam_data.get('children', [])):
                    if pid is None:
                        continue
                    p_data = get_data_from_server(f"{TOP_API_URL}/person/{pid}")
                    if not p_data:
                        continue
                    person = Person(p_data)
                    with tree_lock:
                        if not tree.does_person_exist(person.get_id()):
                            tree.add_person(person)
                    parent_fam = p_data.get('parent_id')
                    if parent_fam and not tree.does_family_exist(parent_fam):
                        fam_queue.put(parent_fam)

            fam_queue.task_done()

    workers = [threading.Thread(target=worker) for _ in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    fam_queue.join()

    # send shutdown
    for _ in workers:
        fam_queue.put(None)

    for w in workers:
        w.join()


# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    fam_queue = queue.Queue()
    fam_queue.put(family_id)

    sem = threading.Semaphore(5)
    tree_lock = threading.Lock()

    def worker():
        while True:
            fam_id = fam_queue.get()
            if fam_id is None:
                fam_queue.task_done()
                break

            sem.acquire()
            try:
                fam_data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
                if fam_data:
                    fam = Family(fam_data)
                    with tree_lock:
                        if not tree.does_family_exist(fam.get_id()):
                            tree.add_family(fam)

                    for pid in (fam_data.get('husband_id'), fam_data.get('wife_id')) + tuple(fam_data.get('children', [])):
                        if pid is None:
                            continue
                        p_data = get_data_from_server(f"{TOP_API_URL}/person/{pid}")
                        if not p_data:
                            continue
                        person = Person(p_data)
                        with tree_lock:
                            if not tree.does_person_exist(person.get_id()):
                                tree.add_person(person)
                        parent_fam = p_data.get('parent_id')
                        if parent_fam and not tree.does_family_exist(parent_fam):
                            fam_queue.put(parent_fam)
            finally:
                sem.release()
                fam_queue.task_done()

    workers = [threading.Thread(target=worker) for _ in range(NUM_WORKERS)]
    for w in workers:
        w.start()

    fam_queue.join()
    for _ in workers:
        fam_queue.put(None)
    for w in workers:
        w.join()
