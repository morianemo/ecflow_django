# ecflow_django

``` sh
export DJANGO_SETTINGS_MODULE="settings"
export PYTHONPATH=$PYTHONPATH:flow:.
virtualenv -p python3 venv
. ./venv/bin/activate
python3 -m pip install --user --upgrade pip
pip install -r requirements.txt
cd flow; python3 ../manage.py runserver 0.0.0.0:8001 

# /usr/local/lib/python3.6/site-packages/ecflow/ecf.py
# ecflow_client is expected to be found in the path, or edit views.py

```
