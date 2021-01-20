from zeep import Client
import concurrent.futures
import time
import pandas as pd
import logging
import os
import sys

start = time.perf_counter()
folders_list = []
directory_list = []
children_list = []
last_modified = []
viz_reports = []
viz_reports_last_modified = []
designer_reports = []
designer_reports_last_modified = []
dashboards = []
dashboards_last_modified = []
images_list = []
images_last_modified = []
dataframes_list = []
subject_areas_dataframes_list = []
config_file = 'config.txt'
log_file = 'logfile.txt'
print("Process Started.......")

try:
    with open(config_file) as f:
        for line in f:
            if line.startswith('URL'):
                url = line.partition('=')[2].strip()
            elif line.startswith('username'):
                username = line.partition('=')[2].strip()
            elif line.startswith('password'):
                password = line.partition('=')[2].strip()
            elif line.startswith('space_ID'):
                space_ID = line.partition('=')[2].strip()
            elif line.startswith('directory'):
                directory = line.partition('=')[2].strip()
            elif line.startswith('excel_workbook'):
                workbook = line.partition('=')[2].strip()
                workbook_name = workbook + '.xlsx'

    # Setting logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter1 = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    formatter2 = logging.Formatter('%(asctime)s:%(levelname)s:%(lineno)d:%(message)s',
                                   datefmt='%m/%d/%Y %I:%M:%S %p')
    filehandler = logging.FileHandler(log_file, mode='w')
    console = logging.StreamHandler()
    filehandler.setFormatter(formatter2)
    filehandler.setLevel(logging.DEBUG)
    console.setFormatter(formatter1)
    console.setLevel(logging.INFO)
    logger.addHandler(filehandler)
    logger.addHandler(console)

    if not url:
        exit_code = 2
        raise RuntimeError('Birst login URL not given..')

    if not username:
        exit_code = 2
        raise RuntimeError('Birst username is not given..')

    if not password:
        exit_code = 2
        raise RuntimeError('Birst password is not given..')

    if not space_ID:
        exit_code = 2
        raise RuntimeError('Birst SpaceID is not given..')

    if not directory:
        exit_code = 2
        raise RuntimeError('Directory is not given..')

    if not workbook:
        workbook_name = 'Directories.xlsx'

except Exception as e:
    logger.error("An unexpected error has occurred initialising input args " + str(sys.exc_info()[0]))
    logger.error(e)
    sys.exit(exit_code)

try:
    wsdl_loc = url + '/' + 'CommandWebService.asmx?wsdl'
    client = Client(wsdl=wsdl_loc)


    def login(username, password):
        login_token = client.service.Login(username, password)
        return login_token


    login_token = login(username, password)

    if login_token:
        logger.info('Birst Login with ' + username + ' Successful')
    else:
        logger.error('Birst Login with ' + username + ' Failed')


    def get_directories(directory1):
        content = client.service.getDirectoryContents(login_token, space_ID, directory1)
        if content['name'] == directory:
            folders_list.append(directory)
            last_modified.append(content['lastModified'])
        if content['isDirectory']:
            try:
                children = content['children']['FileNode']
            except Exception:
                children = []
            if children:
                children_list = []
                for i in range(len(children)):
                    folders_list.append(content['name'] + '/' + children[i]['name'])
                    last_modified.append(children[i]['lastModified'])
                    if children[i]['isDirectory']:
                        children_list.append(directory1 + '/' + children[i]['name'])
                try:
                    if children_list:
                        with concurrent.futures.ThreadPoolExecutor() as executor2:
                            executor2.map(get_directories, children_list)
                except Exception as e:
                    logger.debug(e)
                    # logger.info('Executing in Normal Loop: ')
                    logger.debug("Executing in Normal Loop: ")
                    for j in range(len(children_list)):
                        get_directories(directory1 + '/' + children[j]['name'])


    logger.info("Getting Directories Started...Please Wait")
    logger.info(".....Waiting......")
    get_directories(directory)
    logger.info('Getting Directories Completed...' + ' Time Taken: ' + str(time.perf_counter() - start))
    logger.info(".............")

    for i in range(len(folders_list)):
        if '.dashlet' in folders_list[i]:
            viz_reports.append(folders_list[i])
            viz_reports_last_modified.append(last_modified[i])
        elif '.AdhocReport' in folders_list[i]:
            designer_reports.append(folders_list[i])
            designer_reports_last_modified.append(last_modified[i])
        elif '.page' in folders_list[i]:
            dashboards.append(folders_list[i])
            dashboards_last_modified.append(last_modified[i])
        elif '.png' in folders_list[i]:
            images_list.append(folders_list[i])
            images_last_modified.append(last_modified[i])
        else:
            directory_list.append(folders_list[i])


    def get_directory_permissions(dir1):
        d2 = {}
        grp_name = []
        can_view = []
        can_modify = []
        result = client.service.getDirectoryPermissions(login_token, space_ID, dir1)
        for k in range(len(result)):
            grp_name.append(result[k]['groupName'])
            can_view.append(result[k]['canView'])
            can_modify.append(result[k]['canModify'])
        d2['Directory'] = dir1
        d2['GroupName'] = grp_name
        d2['CanView'] = can_view
        d2['CanModify'] = can_modify
        df2 = pd.DataFrame(d2)
        return df2


    start1 = time.perf_counter()
    logger.info("Getting Directory Permissions Started....Please Wait")
    logger.info(".....Waiting......")
    writer = pd.ExcelWriter(workbook_name, engine='xlsxwriter')

    try:
        logger.info('Getting Directory Permissions...')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(get_directory_permissions, directory_list)
        directory_permissions_sheet = pd.concat([i for i in results], axis=0)
    except Exception as e:
        logger.debug("Executing in Normal Loop")
        logger.debug(e)
        logger.debug('Getting Directory Permissions...')
        for folder in directory_list:
            dataframes_list.append(get_directory_permissions(folder))
        directory_permissions_sheet = pd.concat(dataframes_list, axis=0)

    logger.info('Getting Directory Permissions Completed....' + ' Time Taken: ' + str(time.perf_counter() - start1))
    logger.info('.........')


    def get_customsubjectAreas(spaceID):
        subjectareas_list = client.service.listCustomSubjectAreas(login_token, spaceID)
        return subjectareas_list


    logger.info('Getting Subject Areas Started....')
    subject_areas_list = get_customsubjectAreas(space_ID)
    logger.info('Getting Subject Areas Completed...')
    logger.info('..........')


    def get_subjectarepermissions(name1):
        d1 = {}
        permissions = client.service.getSubjectAreaPermissions(login_token, space_ID, name1)
        d1['SubjectArea'] = name1
        d1['Permissions'] = permissions
        df1 = pd.DataFrame(d1)
        return df1


    logger.info('Getting Subject Area Permissions Started.....')

    try:
        logger.info("Getting Subject Area Permissions...")
        with concurrent.futures.ThreadPoolExecutor() as executor1:
            results1 = executor1.map(get_subjectarepermissions, subject_areas_list)
        subject_areas_dataframe = pd.concat([j for j in results1], axis=0)
    except Exception as e:
        logger.debug('Executing in Normal Loop')
        logger.debug(e)
        logger.debug("Getting Subject Area Permissions....")
        for i in range(len(subject_areas_list)):
            subject_areas_dataframes_list.append(get_subjectarepermissions(subject_areas_list[i]))
        subject_areas_dataframe = pd.concat(subject_areas_dataframes_list, axis=0)

    logger.info('Getting Subject Area Permissions Completed...')
    logger.info('..........')
    logger.info('Exporting the results to Excel WorkBook Started...')

    viz_reports_sheet = pd.DataFrame({'VisualizerReports': viz_reports, 'lastModified': viz_reports_last_modified})
    designer_reports_sheet = pd.DataFrame(
        {'DesignerReports': designer_reports, 'lastModified': designer_reports_last_modified})
    dashboards_sheet = pd.DataFrame({'Dashboards': dashboards, 'lastModified': dashboards_last_modified})
    images_list_sheet = pd.DataFrame({'Images': images_list, 'lastModified': images_last_modified})

    directory_permissions_sheet.sort_values(by='Directory', inplace=True)
    viz_reports_sheet.sort_values(by='VisualizerReports', inplace=True)
    directory_permissions_sheet.to_excel(writer, sheet_name='FolderPermissions', index=False)
    subject_areas_dataframe.to_excel(writer, sheet_name='SubjectAreasPermissions', index=False)
    viz_reports_sheet.to_excel(writer, sheet_name='VisualizerReports', index=False)
    designer_reports_sheet.to_excel(writer, sheet_name='DesignerReports', index=False)
    dashboards_sheet.to_excel(writer, sheet_name='Dashboards', index=False)
    images_list_sheet.to_excel(writer, sheet_name='Images', index=False)

    writer.close()

    logger.info('Exporting the results to Excel Workbook completed....')
    if login_token:
        logger.info('Logout from Birst ...')
        logger.info('Hurray!!! Tool Ran Successfully')
        logger.info('Please Check the results in the Excel WorkBook Created')
        try:
            client.service.Logout(login_token)
        except Exception as e:
            logger.error('Logout from Birst failed')

except Exception as e:
    logger.error('An unexpected error occurred while processing ')
    logger.error(e)
    if login_token:
        logger.info('on error: Logout from Birst ...')
        try:
            client.service.Logout(login_token)
        except Exception as e:
            logger.error('on error: Logout from Birst failed')

os.system('pause')
