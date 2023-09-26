import numpy as np
import trp.trp2 as t2
import trp
import json
import os 
import logging
import math

logger = logging.getLogger(__name__)
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    
def sum_list(num_list):
    a = np.array(num_list)
    return np.sum(a)

def get_res_byte_size(textractRes):
    json_string = json.dumps(textractRes)
    bytes_ = json_string.encode("utf-8")
    return len(bytes_)

class AnalyzeTextract:
    def __init__(self, textract_json):
        log_level = os.environ.get('LOG_LEVEL', 'DEBUG')    
        
        logger.setLevel(log_level)
        self.ocr_output = textract_json
        self.t_document = t2.TDocumentSchema().load(self.ocr_output)
        self.trp_doc = trp.Document(t2.TDocumentSchema().dump(self.t_document))
        self.blocks = 0
        self.characters = 0
        self.words = 0
        self.lines = 0
        self.tables = 0
        self.forms = 0
        self.pages = 0
        self.size = 0
        self.paginator = 0
    
        num_lines = []
        num_tables = []
        num_words = []
        num_char = []
        num_form_fields = []
        num_blocks = []
        
        for page in self.trp_doc.pages:
            lines = list(page.lines)
            words = [word for line in lines for word in line.words]
            char = [char for word in words for char in word.text]

            num_char.append(len(char))
            num_words.append(len(words))
            num_lines.append(len(lines))
            num_blocks.append(len(page.blocks))

            if hasattr(page, "tables"):
                num_tables.append(len(page.tables))
            else:
                num_tables = [0]
            if hasattr(page, "form"):
                num_form_fields.append(len(page.form.fields))
            else:
                num_form_fields = [0]

        self.blocks = sum_list(num_blocks)
        self.characters = sum_list(num_char)
        self.words = sum_list(num_words)
        self.lines = sum_list(num_lines)
        self.tables = sum_list(num_tables)
        self.forms = sum_list(num_form_fields)
        self.pages = len(self.trp_doc.pages)
        self.size = get_res_byte_size(self.ocr_output)
        self.paginator = math.ceil((self.blocks/1000))
    def metrics_to_json(self):
        
        json_res = {
            "blocks": self.blocks,
            "characters": self.characters,
            "words": self.words,
            "lines": self.lines,
            "tables": self.tables,
            "forms": self.forms,
            "pages": self.pages,
            "size": self.size,
            "paginator": self.paginator
        }

        logger.info(json.dumps(json_res, cls=NpEncoder))
        return json.dumps(json_res, cls=NpEncoder)                                   