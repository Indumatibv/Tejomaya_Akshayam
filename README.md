# Tejomaya_Akshayam


# Python version
python --version

# Create & activate env
conda create -n Tejomaya python=3.10.19 -y
conda activate Tejomaya

# Install dependencies
pip install -r requirements.txt
pip install "unstructured[all-docs]"  nltk

# Install Playwright browsers
playwright install

# (Optional) NLTK data
python -m nltk.downloader punkt stopwords

# Verify LangChain
python - <<EOF
import langchain
print(langchain.__version__)
from langchain.llms import Ollama
from langchain.prompts import PromptTemplate
print("OK")
EOF
-------------

API for manual triggers
uvicorn api.main:app --reload


Scheduler for auto triggers
python scheduler/scheduler.py
