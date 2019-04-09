#!/usr/bin/python3

#a quick and dirty script to scrape/harvest resource-level metadata 
#the original purpose of this work is to support the ongoing international city open data index project led by SASS

import requests

result = requests.get('https://data.cityofnewyork.us/data.json')
package_list = result.json()['dataset']

# iterate the returned package name list to fetch package details
for p in package_list:
    id_url = p['identifier']
    result = requests.get(id_url)
    print id_url
    package = result.json()
    #socrata provides two api, one is data.json and the other is data metadata json
    #the data.json contains data's metadata in human-readable format but may miss some metadata

    #get the package_name from data.json
    package_name = '"'+p['title']+'"'
    print package_name
    #org is fetched from data metadata json, and maybe missing
    #get frequency from data metadata json, and maybe missing
    package_org = 'MISSING'
    package_frequency = 'MISSING'
    metadata_key = package.get('metadata','')
    if metadata_key != '':
        custom_fields = metadata_key.get('custom_fields','')
    if custom_fields != '':
        dataset_info = custom_fields.get('Dataset Information','')
        update = custom_fields.get('Update','')
        if dataset_info != '':
            package_org = '"'+dataset_info['Agency']+'"' if dataset_info.get('Agency','') else 'MISSING'
        if update != '':
            package_frequency = '"'+update['Update Frequency']+'"' if update.get('Update Frequency','') else 'MISSING'
    #get the description from data.json
    #replace quotes with star and linebreaker with double space in the scraped description text for easy store in csv
    package_desc = p['description'].replace('"',"*").replace("\n","  ") if p['description'] else 'BLANK'
    #get the topics from data.json, may have multiple topics
    package_topics = '"'+'|'.join(p['theme'])+'"' if p.get('theme','')!= '' else 'MISSING'
    #get the topics from data.json, may have multiple topics
    package_tags = '|'.join(p['keyword']) if p.get('keyword','')!= '' else 'MISSING'
    #get the date from data.json
    package_created = p['issued'] if p['issued'] else 'BLANK'
    package_updated = p['modified'] if p['modified'] else 'BLANK'

    #socrata supports visitcount, downloadcount
    package_view = package['viewCount']
    package_download = package['downloadCount']
    #socrata supports row num and column num
    package_column = len(package['columns'])
    try:
        package_format = []
        distribution = p['distribution']
        for d in distribution:
            package_format.append(d['mediaType'])
        package_format = '|'.join(package_format)
    except Exception as ex:
        print(ex)
        package_format = 'MISSING'


    #additional information we record for further investigation
    package_id = package['id']
    print package_id
    package_avg_rating = package['averageRating']
    package_hideFromCatalog = package['hideFromCatalog']
    package_hideFromDataJson = package['hideFromDataJson']
    package_numberOfComments = package['numberOfComments']
    package_type = p['@type']
    package_displayType = package['displayType']

    #to obtain the row count , we use SOAD api
    api_url = "https://data.cityofnewyork.us/resource/"+package_id+".json?$select=count(*)"
    print api_url
    result = requests.get(api_url)
    try:
        package_row = result.json()[0]['count']
    except Exception as ex:
        print ex
        package_row = 'SODA-NOT-FOUND'

    #package detail + resource detail as one row. write it into file as csv
    row = package_id+','+package_type+','+package_displayType+','+package_name+','+'"'+package_desc+'"'+','+package_org+','+package_topics \
        +','+package_tags+','+package_format+','+package_created+','+package_frequency+','+package_updated \
        +','+str(package_view)+','+str(package_download)+','+str(package_row)+','+str(package_column) \
        +','+str(package_avg_rating)+','+str(package_numberOfComments)+','+str(package_hideFromCatalog)+','+str(package_hideFromDataJson)+'\n'
    print row
    
    package_dict = {
                    'id':package_id,
                    'type':package_type,
                    'displayType':package_displayType,
                    'name':package_name,
                    'desc':package_desc,
                    'org':package_org,
                    'topics':package_topics,
                    'tags':package_tags,
                    'format':package_format,
                    'created':package_created,
                    'frequency':package_frequency,
                    'updated':package_updated,
                    'view':package_view,
                    'download':package_download,
                    'row':package_row,
                    'column':package_column,
                    'avgrating':package_avg_rating,
                    'numofcomments':package_numberofComments,
                    'hidefromcatalog':package_hideFromCatalog,
                    'hidefromdatajson':package_package_hideFromDataJson,   
    }
    
    scraperwiki.sqlite.save(unique_keys=['id'],data=package_dict)

    print('****************end---'+package_name+'---end****************')
#close the file
csv_file.close()
