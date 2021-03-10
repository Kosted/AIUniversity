from datetime import datetime

from history import ExtendHistory


def main():
    start_time = datetime.now()

    history_obj = ExtendHistory()

    while history_obj.check_size()[1] != 3:
        history_obj.insert_new_history()

    print(f'Time: {datetime.now() - start_time}. dupe: {history_obj.dupe_count} Size: {history_obj.translate_to_gb()}')

    filepath = 'history_dump'
    history_obj.save_history(filepath)

    history_obj.load_history(filepath)

    start_time_after_3gb = datetime.now()

    while history_obj.check_size()[1] != 5:
        history_obj.insert_new_history()

    print(
        f'Time: {datetime.now() - start_time_after_3gb}. dupe: {history_obj.dupe_count} Size: {history_obj.translate_to_gb()}')


if __name__ == "__main__":
    main()

