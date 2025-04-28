from tqdm import tqdm

print = tqdm.write

def tqdm_range(
    iterable,
    desc: str = "",
    category_width: int = 12,
):
    return tqdm(range(len(iterable)), desc=f"[{desc}]".ljust(category_width))
