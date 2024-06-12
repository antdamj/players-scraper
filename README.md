
# Player scraper task solution

Solution for entry task for Data Engineer position in C&I.



## Usage/Examples

All project dependencies are in `requirements.txt`.
Project entrypoint is in `main.py`. Run it in three different configurations:

- Without any specific parameters - URLs will be loaded from the csv provided in task;
- With CSV provided - make sure the file is placed inside the `data` subdirectory;
- With CSV and "restart" flag - you can opt in to restart the saved database by simply passing "Y" or "Yes" (or any word starting in "Y" or "y") - false by default.

Examples provided below:

```console
./pip install requirements.txt

./python main.py
./python main.py playersURLs_alternative.csv
./python main.py playersURLs_alternative.csv Yes
```


## Authors

- [@antdamj](https://www.github.com/antdamj)

