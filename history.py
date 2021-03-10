import pandas as pd
import random


class History:

    def __init__(self):
        self.history_arr = pd.Series(dtype=int)
        self.dupe_count = 0
        self.score = float(999999999999999000000)
        self.history_len = 0
        # settings
        self.sequence_len = 500
        self.MAX_INT = 9223372036854775807

    def set_history(self, sequence, score):
        if self.is_it_dupe_sequence(sequence):
            self.dupe_count += 1
            if score > self.score:
                self.score = score
        else:
            self.history_arr = self.history_arr.append(sequence)
            self.history_len += len(sequence)

    def is_it_dupe_sequence(self, sequence):
        inter_points = self.history_arr[self.history_arr == sequence.values[0]]
        for inter_point in inter_points.index:
            equal_result = sequence.equals(self.history_arr[inter_point:inter_point+500])
            if equal_result:
                return True
        return False

    def save_history(self, filepath):
        pd.to_pickle(self.history_arr, f"{filepath}.pkl")

    def load_history(self, filepath):
        self.history_arr = pd.read_pickle(f"{filepath}.pkl")


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



