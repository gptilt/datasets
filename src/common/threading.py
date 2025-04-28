import threading
from .tqdm import print

def multithreaded(
    iterable: iter,
    target: callable,
    args: tuple = (),
    kwargs: dict = {},
):
    list_of_threads = []

    for iteration in iterable:
        print(f"[{iteration}] Starting thread...")
        list_of_threads.append(threading.Thread(
            target=target,
            args=(iteration, *args),
            kwargs=kwargs
        ))
    
    for t in list_of_threads:
        t.start()

    for t in list_of_threads:
        t.join()

    print("All threads have finished.")
