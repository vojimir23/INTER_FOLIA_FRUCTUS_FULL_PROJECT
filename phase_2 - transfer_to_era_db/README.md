# Project Documentation: ERA Entity and Relation Automation

## Overview
This project automates the processing and management of entities and relationships using the ERA API. It reads Excel files containing entity and relationship information, processes the data, and sends the appropriate requests to the ERA server to either create or update entities and relationships.

The system is divided into several Python scripts that perform specific functions, such as handling configuration settings, command-line arguments, API interaction, and data processing.

## Project Structure
- `__main__.py`: The main entry point of the project that orchestrates the entity and relationship processing.
- `cli.py`: Handles command-line arguments to accept the path to the Excel file for conversion.
- `config.py`: Loads configuration settings from a `recipe.toml` file, such as server details, entity mappings, and relationships.
- `mangopie.py`: Contains the `Mango` class that interacts with the ERA server for entity and relationship CRUD operations via REST API calls.
- `spoon.py`: Contains utility functions for processing fields, such as splitting delimited strings.
- `recipe.toml`: Stores configuration details, such as the server settings, entity mappings, and relations.

## Dependencies
This project uses the following Python libraries:
- `pandas`: For data manipulation and handling Excel sheets.
- `alive-progress`: To display progress bars for entity and relationship processing.
- `requests`: To send HTTP requests to the ERA server.
- `numpy`: For efficient handling of array data.
- `argparse`: For command-line argument parsing.

## Installation
To set up the environment, follow these steps:

1. **Clone the repository**:
   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create a virtual environment**:
   ```sh
   python -m venv venv
   ```

3. **Activate the virtual environment**:
   - On Windows:
     ```sh
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```sh
     source venv/bin/activate
     ```

4. **Install the required dependencies**:
   ```sh
   pip install -r requirements.txt
   ```

## Usage
To run the script, provide the path to the Excel file you want to process:

```sh
python __main__.py /path/to/excel_file.xlsx
```

The script will:
1. Load the Excel file and concatenate all sheets.
2. Remove duplicate rows.
3. Authenticate with the ERA server.
4. Process and create entities and relations as per the configuration.
5. Save the entities and relations to JSON files in the `output` directory.

## Script Details
### `__main__.py`
- Loads Excel sheets and processes them into a DataFrame.
- Uses the `Mango` class from `mangopie.py` to interact with the ERA API.
- Processes entities and relations using multithreading (`ThreadPoolExecutor`) to improve performance.
- Uses progress bars (`alive-progress`) to show the processing status.

### `mangopie.py`
- Defines the `Mango` class, which includes methods such as:
  - `authenticate()`: Authenticates to the ERA server and stores a JWT token.
  - `get_active_entities()`, `get_relationTypes()`, `get_relations()`: Fetches active entities and relationships.
  - `merge_entity()`, `update_entity()`, `merge_relation()`: Manages entities and relationships by either creating or updating them.
- Uses the `requests` library for all API interactions.

### `spoon.py`
- Contains the function `process_field()`, which processes fields by splitting strings based on a specified delimiter and normalizing the data (`lowercase`, `strip whitespace`).
- Useful for transforming complex field data into a form suitable for API requests.

### `cli.py`
- Uses `argparse` to handle command-line arguments.
- Expects a single argument: the path to the Excel file to be processed.

### `config.py`
- Loads configuration settings from a TOML file (`recipe.toml`).
- Provides configurations such as:
  - `server` details for the ERA API.
  - `column2type`, `properties`, and `relations` mappings to guide the entity and relationship creation process.

## Configuration
### `recipe.toml`
The `recipe.toml` file contains all the configuration needed for processing:
- **`server`**: Stores details like the server URL and credentials.
- **`column2type`**: Defines the mapping between Excel columns and entity types.
- **`properties`**: Lists additional properties for entities.
- **`relations`**: Specifies how relationships should be formed between entities.

## Example Workflow
1. **Prepare an Excel File** with data representing entities and relationships.
2. **Run the Script** to process the file.
3. **Review Outputs**:
   - JSON files (`entities.json` and `relations.json`) are saved in the `output` folder.
   - These files contain detailed information about the created or updated entities and relationships.

## Notes
- The script uses multithreading to speed up the processing of entities and relations, making it suitable for larger datasets.
- It handles authentication securely using bearer tokens, which are reused for subsequent API requests.
- The progress bars provide real-time status updates to indicate the completion of various stages in entity and relation creation.
