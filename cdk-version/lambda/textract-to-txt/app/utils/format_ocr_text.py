import os
import logging
from statistics import median
import math

logger = logging.getLogger(__name__)

class FormatOCR:
    def __init__(self, j):
        log_level = os.environ.get('LOG_LEVEL', 'DEBUG')    
        logger.setLevel(log_level)
        self.ocr_output = j
        self.Blocks = j.get('Blocks', [])
    
    def json_to_tables(self):        
        return [block for block in self.Blocks if block.get('BlockType') in ['TABLE', 'CELL','TABLE_TITLE']]
    
    def json_to_forms(self):        
        return [block for block in self.Blocks if block.get('BlockType') == 'KEY_VALUE_SET']        

    def json_to_text(self):
        y_threshold = 0.01
        blocks = self.ocr_output.get('Blocks', [])

        # Extract LINE and PAGE blocks
        lines = [block for block in blocks if block.get('BlockType') == 'LINE']
        page = [block for block in blocks if block.get('BlockType') == 'PAGE'][0]

        # Calculate the skew angle of the document
        p1, p2 = page['Geometry']['Polygon'][:2]
        skew_angle = math.atan((p1['Y'] - p2['Y']) / (p1['X'] - p2['X']))

        # Adjust the 'Top' positions of the lines to deskew them
        for line in lines:
            line['Geometry']['BoundingBox']['Top'] -= line['Geometry']['BoundingBox']['Left'] * math.tan(skew_angle)

        # Sort blocks by top position
        sorted_lines = sorted(lines, key=lambda x: x['Geometry']['BoundingBox']['Top'])

        # Calculate the differences between 'Top' values of consecutive lines
        diffs = [sorted_lines[i+1]['Geometry']['BoundingBox']['Top'] - sorted_lines[i]['Geometry']['BoundingBox']['Top'] for i in range(len(sorted_lines)-1)]

        # Use the median difference as the Y threshold
        if diffs:
            y_threshold = median(diffs)

        # default y_threshold if it is less than or equal to 0.01
        if y_threshold <= 0.01:
            y_threshold = 0.01
      
        # Group lines with similar Y positions together
        line_groups = []
        current_group = [sorted_lines[0]]
        for line in sorted_lines[1:]:
            # If the line's Y position is close to the current group's Y position,
            # add it to the current group
            if abs(line['Geometry']['BoundingBox']['Top'] - current_group[0]['Geometry']['BoundingBox']['Top']) < y_threshold:
                current_group.append(line)
            else:
                # Otherwise, start a new group
                line_groups.append(current_group)
                current_group = [line]
        line_groups.append(current_group)  # add the last group

        # Sort lines within each group by left position, and concatenate the text
        text = ''
        for group in line_groups:
            sorted_group = sorted(group, key=lambda x: x['Geometry']['BoundingBox']['Left'])
            for line in sorted_group:
                text += line.get('Text', '') + '\t'
            text += '\n'  # start a new line for the next group

        return text.strip(), sorted_lines