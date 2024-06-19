from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
import pandas as pd
import openai
import os


openai.api_key = os.getenv("OPENAI_API_KEY")  # API key

# defining input and output folder paths as before
inputFolderPath = r'C:\InternCSVs\GIS_Web_Services'
outputFolderPath = r'C:\InternCSVs\GIS_Web_Services\output'
os.makedirs(outputFolderPath, exist_ok=True)

# Borrowed from the example code posted. This is a revised script from CSVReader. I've taken out the
# containsRelevantTags function to simplify things. The purpose of this script is ONLY to have the ChatOpenAI llm
# write decriptions for CSV entries where the description is null based on what it finds in the title, url, and tags
# columns and give n energy relevancy score from 1 to 10 based on a list of tags.
tagging_prompt = ChatPromptTemplate.from_template(
    """
Write the descriptions based on keywords you find in the title, url, and tags columns of the CSV files.

Only extract the properties mentioned in the 'Classification' function.

Passage:
{input}
"""

)


# Borrowed from the example code
class Classification(BaseModel):

    # A Field to hold the description the ChatOpenAI llm comes up with. I think??
    generatedDescription: str = Field(description="Write a one-sentence description generated based on the title, "
                                                   "URL, and tags.")

    # A Field to hold the relevance score on a scale from 1 to 10 for each entry in the CSV file based on the tags???
    energy_related: int = Field(description="How related is this to energy from 1 to 10")

    # these tags represent content related to specific issues. "Wells" might be related to oil drilling/exploration ??
    tag: str = Field(..., enum=[
        "environment", "leases", "blocks", "licenses", "bathymetry", "wells",
        "pipelines", "infrastructure", "imagery", "weather", "geology", "seismic",
        "emissions", "topography", "geomatics", "renewables"
    ])


# Instantiating the ChatOpenAI llm https://python.langchain.com/v0.2/docs/integrations/llms/openai/ using the
# "with_structured_output fucntion (CLassification as parameter) from the example code.
llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0, max_tokens=None,
                 timeout=None).with_structured_output(Classification)


chunk_size = 206  # 206 is the first entry with a null description. Trying to see if things break down here.

for filename in os.listdir(inputFolderPath):  # loop through files in the input folder
    if filename.endswith('.csv'):    # make sure it's a CSV file
        csvFilePath = os.path.join(inputFolderPath, filename)
        for chunk in pd.read_csv(csvFilePath, chunksize=chunk_size):    # use pandas to red the CSV at the csvFilePath
            for index, row in chunk.iterrows():    # chunk is the dataframe object in this case. Start looping over the df.
                if pd.isnull(row['description']):  # check if the description is null
                    title = row['title']   # create variables for the necessary rows
                    url = row['url']
                    tags = row['tags']

                    #tagging_chain = tagging_prompt | llm   # honestly pretty lost here

                    # inp = f"Description: "

                    # response = llm.invoke({"input": inp})

                    # Save updated chunk to output CSV
            outputCSVFilePath = os.path.join(outputFolderPath, filename)
            chunk.to_csv(outputCSVFilePath, index=False)
            print(f"Processed chunk of DataFrame for {filename}:")
            print(chunk)
