# PostgreSQL Database Synchronization Tool

This project provides a tool for synchronizing data between two PostgreSQL databases. It is designed to facilitate the transfer of data from a source database to a target database, ensuring that both databases remain in sync.

## Project Structure

```
pg-db-sync
├── src
│   ├── main.py               # Entry point of the application
│   ├── db_connector.py       # Handles database connections
│   ├── sync_manager.py       # Manages the synchronization process
│   ├── utils
│   │   ├── __init__.py       # Initializes the utils package
│   │   ├── config.py         # Loads and parses configuration settings
│   │   └── logger.py         # Provides logging functionality
│   └── models
│       ├── __init__.py       # Initializes the models package
│       └── table_mapping.py   # Defines table mapping between databases
├── config
│   └── config.json           # Configuration settings for database connections
├── requirements.txt          # Project dependencies
├── .env.example              # Example environment variables
└── README.md                 # Project documentation
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/pg-db-sync.git
   cd pg-db-sync
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure the database connections by editing the `config/config.json` file with your database credentials and synchronization parameters.

4. Optionally, create a `.env` file based on the `.env.example` file to set environment variables.

## Usage

To run the synchronization process, execute the following command:
```
python src/main.py
```

This will initialize the database connections and trigger the synchronization process between the source and target databases.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.