import pandas as pd
import random
from multiprocessing import Pool


class History:

    def __init__(self):
        self.history_arr = pd.Series(dtype=int)
        self.dupe_count = 0
        self.score = float(999999999999999000000)
        self.history_len = 0
        # settings
        self.sequence_len = 500
        self.MAX_INT = 9223372036854775807
        self.processor_num = 8

    def set_history(self, sequence, score):
        split_scope = self.split_history(self.processor_num)
        self.last_sequence = sequence
        with Pool(self.processor_num) as p:
            dupe_flags = p.map(self.is_it_dupe_sequence, split_scope)
        if any(dupe_flags):
            self.dupe_count += 1
            if score > self.score:
                self.score = score
        else:
            self.history_arr = self.history_arr.append(sequence)
            self.history_len += len(sequence)

    def is_it_dupe_sequence(self, split_scope):
        history_part = self.history_arr[split_scope[0]:split_scope[1]]
        inter_points = history_part[self.history_arr == self.last_sequence.values[0]]
        for inter_point in inter_points.index:
            equal_result = self.last_sequence.equals(history_part[inter_point:inter_point + 500])
            if equal_result:
                return True
        return False

    def save_history(self, filepath):
        pd.to_pickle(self.history_arr, f"{filepath}.pkl")

    def load_history(self, filepath):
        self.history_arr = pd.read_pickle(f"{filepath}.pkl")
        self.history_len = len(self.history_arr)

    def split_history(self, processor_num):
        history_part = self.history_len // processor_num
        scopes = list()
        for i in range(1, processor_num+1):
            if i == 1:
                left = 0
            else:
                left = history_part * (i - 1) - 499 - processor_num

            if i == processor_num:
                right = self.history_len
            else:
                right = history_part * i + 499 + processor_num
            scopes.append((left, right))
        return scopes


class ExtendHistory(History):

    def __init__(self):
        super(ExtendHistory, self).__init__()
        self.GB = 1024 * 1024 * 1024

    def check_size(self):
        size = self.history_arr.memory_usage(index=False)
        target = None
        if size > 5 * self.GB:
            target = 5
        if size > 3 * self.GB:
            target = 3
        return size, target

    def random_sequence_and_score(self):
        return pd.Series([random.randint(0, self.MAX_INT) for _ in range(self.sequence_len)],
                         index=range(self.history_len, self.history_len + self.sequence_len)), random.random()

    def translate_to_gb(self):
        return round(self.check_size()[0] / self.GB, 3)

    def insert_new_history(self):
        self.set_history(*self.random_sequence_and_score())


if __name__ == "__main__":
    history = ExtendHistory()
    history.load_history('first_save')
