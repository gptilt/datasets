from tqdm import tqdm

print = lambda x: tqdm.write(str(x))

def tqdm_range(
    iterable,
    desc: str = "",
    category_width: int = 12,
    step: int = 1
):
    return tqdm(range(0, len(iterable), step), desc=f"[{desc}]".ljust(category_width))


def work_generator(
    list_of_work_pieces: list,
    work_function: callable,
    descriptor: str,
    max_count: int = -1,
    step: int = 1,
    *args,
    **kwargs
):
    """
    Generates pieces of data using a work function provided by the caller.

    Args:
        list_of_work_pieces (list): List of things to work on.
        work_function (function): Function that produces data from a work piece.
        descriptor (str): Descriptor for the tqdm range.
        max_count (int, optional): Maximum number of data pieces to produce.
    Yields:
        count (int): Count of work pieces successfully processed.
        data (any): For each work piece.
    """
    real_count = 0
    for i in tqdm_range(list_of_work_pieces, desc=descriptor, step=step):
        if real_count == max_count:
            continue
        data = work_function(
            list_of_work_pieces[i:i+step] if step > 1 else list_of_work_pieces[i],
            descriptor,
            *args,
            **kwargs
        )
        if data is not None:
            real_count += 1
            yield data
