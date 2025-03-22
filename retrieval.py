# retrieval.py
from langchain.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain.embeddings.openai import OpenAIEmbeddings
from config import OPENAI_KEY

def setup_retriever(path="551_sql_query_examples.txt"):
    raw_documents = TextLoader(path).load()
    splitter = CharacterTextSplitter(chunk_size=250, chunk_overlap=0)
    documents = splitter.split_documents(raw_documents)
    db = FAISS.from_documents(documents, OpenAIEmbeddings(openai_api_key=OPENAI_KEY))
    return db.as_retriever()
