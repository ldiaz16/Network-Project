from src.load_data import DataStore

def main():
    data_storage = DataStore()
    data_storage.load_data()
    data_storage.user_airline()

if __name__ == "__main__":
    main()
