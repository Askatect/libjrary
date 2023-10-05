import logger
import logging
from pretty_html_table import build_table
import pandas as pd
from formatting import jsonformatterv2 as jsonformatter
from formatting import sqlformatterv5 as sqlformatter
import pyjap.SQLHandler as SQLHandler

logging.info("Testing!")

sqlcon = SQLHandler.SQLHandler('GDBA_sit')
sqlcon.connect_to_sqlserver()
sqlcon.execute_query("""
PRINT('Hello world!')
WAITFOR DELAY '00:01:00'
PRINT('Goodbye world.')
""")
print(sqlcon.cursor.messages)
sqlcon.cursor.nextset()
print(sqlcon.cursor.messages)
sqlcon.close_connection()

# df = pd.read_csv("C:/Users/JoshAppleton/Downloads/goodreads_library_export.csv",
#                  index_col = 'Book Id').head()
# print(df.head())

# for index, row in df.iterrows():
    # print(f"Book Id {index} is {row['Title']}.")

# table = build_table(df,
#                     'grey_dark',
#                     font_size = 'medium',
#                     font_family = 'arial',
#                     even_bg_color = '#380435',
#                     odd_bg_color = '#FF00F8',
#                     even_color = '#FFFFFF',
#                     text_align = 'center')

# with open('table.html', 'w') as f:
#     f.write(table)

# import math

# def f(n):
#     root2 = math.sqrt(2)
#     A = root2 + 9/4
#     B = 9/4 - root2
#     return (root2**n)*(A + ((-1)**n)*B) - n - 2

# b = []
# for n in range(1, 10):
#     b.append(math.trunc(f(n)))
# print(b)

# b = [1, 5]
# for n in range(0, 7):
#     b.append(2*b[n] + n + 1)
# print(b)

# json = """
# {     "external_id":"FETCH_Contact_Number__c",     "dependent_key":"business_key",     "object_name":"Contact",     "email_fields":[        {           "email":"email"        }     ],     "postcode":"postcode",     "loading":[        {           "businesskey_source":"FETCH_Contact_Number__c",           "businesskey_target":"contact_number"        }     ],     "schema":[        {           "AddressPrimaryType__c":"address_type",           "AddressPrimaryHouseName__c":"house_name",           "AddressPrimaryLine1__c":"address_line1",           "AddressPrimaryLine2__c":"address_line2",           "AddressPrimaryLine3__c":"address_line3",           "AddressPrimaryLine4__c":"address_line4",           "AddressPrimaryTown__c":"town",           "AddressPrimaryCounty__c":"county",           "AddressPrimaryPostCode__c":"postcode",           "AddressPrimaryCountry__c":"country",           "AddressPrimaryUKCountry__c":"uk_country",           "Anonymous__c":"anonymous",           "Areas_of_Interest__c":"areas_of_interest",           "Birthdate":"date_of_birth",           "Contact_Record_Type__c":"contact_type",           "Contains_Critical_Information__c":"contains_critical_info",           "Critical_Information__c":"critical_info",           "Deceased__c":"deceased",           "Deceased_Confirmed__c":"deceased_confirmed",           "Do_Not_Want_Ineligible_for_GA__c":"do_not_want_ineligible_for_ga",           "Email":"email",           "Events_Blacklist__c":"event_blacklist",           "FETCH_Contact_Number__c":"business_key",           "FETCH_Source_Date__c":"source_date",           "FirstName":"forenames",           "Form_Filled_Date__c":"form_filled_date",           "Gender__c":"sex",           "Helping_Guide_Dogs_Form__c":"helping_guide_dogs_form",           "HomePhone":"homephone",           "Honorifics__c":"honorifics",           "Initials__c":"initials",           "Known_As__c":"preferred_forename",           "LastName":"surname",           "Mailing_Date__c":"mailing_date",           "Mdw_Integration_Id__c":"business_key",           "Medical_Details__c":"medical_details",           "MobilePhone":"mobilephone",           "No_Gift_Aid_Reason__c":"no_gift_aid_reason",           "npe01__Primary_Address_Type__c":"address_type",           "PAM_Status__c":"pam_status",           "Preferred_Correspondence_Medium__c":"preferred_correspondence_medium",           "Salutation":"title",           "Seed_Record_Mail_Email__c":"seed_record_mail_email__c",           "Seed_Record_Phone__c":"seed_record_phone__c",           "Source_Code__c":"source",           "Title":"salutation"        }     ]  }
# """

# print(jsonformatter(json))
logging.info('Complete!')

