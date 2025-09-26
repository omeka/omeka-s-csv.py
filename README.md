# omeka-s-csv.py

omeka-s-csv.py is a Python script that exports data from Omeka S installations in CSV format.

## Requirements

omeka-s-csv.py works with either Python 2 or 3, and requires no additional or external dependencies.

Linux users will likely have Python already installed, and packages should be available from your distribution. There are several options for installing Python on Windows and Mac, including downloading an installer [from Python's website](https://www.python.org/downloads/).

## Usage

### Interactive mode

In a terminal in the folder where omeka-s-csv.py is located on your system, run the script:

```
python3 omeka-s-csv.py
```

(this can also be done, depending on the system, as `./omeka-s-csv.py`, `python omeka-s-csv.py`, or `python2 omeka-s-csv.py`.)

The script will prompt you for the Omeka S API endpoint you want to export from. This is a URL pointing to the target Omeka S installation, and should start with `http://` or `https://` end with `/api`.

Next, you will be prompted for an API key. Keys are required to export non-public data from the site, but are optional if you're only exporting public data. If you're not using a key, you can leave the input blank and just press Enter.

Finally, a prompt will ask you to enter a "separator," a character that's used to separate multiple pieces of data in a single CSV cell. It's important that the chosen separator is a character that doesn't appear in the actual data. The default choice is the "pipe" character (`|`), which is usually a safe option. To use that default choice, just press Enter, otherwise type the separator character you want to use instead.

The script will then run, and show output indicating its progress. The exported results will be placed in files in the same folder as the omeka-s-csv.py file: `items.csv`, `item_sets.csv`, and so on.

### Command-line flags

As an alternative to interactive mode, you can call the script with command-line flags to specify the export settings you want without following the prompts. This is useful if you want to do the same export multiple times as you can just save and re-run the command with your desired flags, and this mode also offers some features not available in interactive mode.

The most basic way to run the script in this mode is to call it and also pass the URL to the Omeka S API endpoint as an argument:

```
python3 omeka-s-csv.py https://example.com/api
```

That will perform an anonymous export with no API key, and will use the default multivalue separator of the pipe (`|`). (The other ways of calling the script mentioned in the previous section are valid here as well.)

Optional flags allow you to customize other export settings:

#### Resource

By default, the export will run over several different resource types and create multiple files: one each for items, item sets, and media. Passing `--resource <resource_name>` will just perform an export of that one resource type alone.

#### Query

If you've selected a resource type with `--resource`, you can filter what's exported by using `--query` and passing a query string. Using this, you can filter your export to any result set that the API supports.

For example, to export only the items that are in the item set with ID 10:

```
python3 omeka-s-csv.py https://example.com/api --resource items --query "item_set_id=10"
```

#### Filename

If you've selected a resource type with `--resource`, you can choose the path where the exported CSV file will be saved by passing `--filename <path>`.

For example, to export all items to a file named "custom.csv":

```
python3 omeka-s-csv.py https://example.com/api --resource items --filename custom.csv
```

#### Multivalue Separator

You can change the multivalue separator from the default by passing `--multivalue-separator <separator>`.

#### API Key

You can use an API key for an authenticated export by passing `--key-identity <identity>` and `--key-credential <credential>`. You must pass both of these together to use a key; passing just one will result in an error.
