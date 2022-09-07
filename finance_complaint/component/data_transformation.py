from finance_complaint.entity.complaint_column import ComplaintColumn
from finance_complaint.exception import FinanceException
import sys





class DataTransformation(ComplaintColumn):

    def __init__(self, ):
        super().__init__()




    def initiate_data_transformation(self):
        try:
            pass

        except Exception as e:
            raise FinanceException(e,sys)
