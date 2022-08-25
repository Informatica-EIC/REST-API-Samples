# Excel import into EDC

this folder is used to store files referenced by the youtube video https://www.youtube.com/watch?v=ztiJZEKNufI

the original link posted is no longer valid (was a public s3 bucket) - the individual files references & the .zip are now located here.

# Implementation Note:

- the example was created in 2019 & is still valid with current versions
- there are some newer api's available that may be better/faster
  - PATCH /2/catalog/data/objects/{id}  to update an object  (does not require an eTag)
  - POST /2/catalog/jobs/objectImports - Catalog Object Import - bulk import (same as the csv export/import from EDC ui)

for any problems, raise an issue in this github repository