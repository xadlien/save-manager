import boto3
import hashlib
import os


class Save:

    def __init__(self, local_path):
        self.local_path = os.path.expanduser(local_path) + ("/" if local_path[-1] != "/" else "")
        self.index = {}

    def get_local_file_list(self):

        # init return lists
        folder_list = []
        file_list = []

        # if the directory doesn't exist create it
        if not os.path.exists(self.local_path):
            os.mkdir(self.local_path)

        # if path is a file, just return the file + hash
        if not os.path.isdir(self.local_path):
            file_hash = hashlib.md5(open(self.local_path,'rb').read()).hexdigest()
            file_list = [(self.local_path, file_hash)]
            return folder_list, file_list

        # recursively get folders/files + hashes
        return self._get_local_file_list(self.local_path)

    def _get_local_file_list(self, path):

        path = path.replace('//', '/')
        
        # init lists
        folder_list = []
        file_list = []

        # if its a file just return the lists
        if os.path.isfile(path):
            file_hash = hashlib.md5(open(path,'rb').read()).hexdigest()
            return folder_list, [(path, file_hash)] 
        
        # if it is a folder, save the folder name and list contents
        if os.path.isdir(path):

            list_objects = os.listdir(path)
            folder_list.append(path)
            for obj in list_objects:
                new_folder_list, new_file_list = self._get_local_file_list(path + "/" + obj)
                folder_list.extend(new_folder_list)
                file_list.extend(new_file_list)

        return folder_list, file_list
    

class AWSSave(Save):

    def __init__(self, access_key, secret_key, uri, local_path):

        super().__init__(local_path)

        # check that the protocol is S3
        protocol = uri.split(":")[0]
        if protocol != "s3":
            print(f"ERROR: PROTOCOL is not s3, received {protocol}")
            exit(1)

        # connect to aws
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        # set bucket name
        self.bucket_name = uri.split("/")[2]
        self.path = "/".join(uri.split("/")[3:])
 
    def save(self):

        # get the list of folders and files to copy 
        _, file_list = self.get_local_file_list()
        file_dict = {}
        for file_tup in file_list:
            file_dict[file_tup[0]] = file_tup[1]

        # get the previous index if there
        try:
            index_object = self.client.get_object(Bucket=self.bucket_name, Key=f"{self.path}/index")
        except:
            index_object = None
        
        # parse the data
        self.parse_index(index_object)

        # list files from index if they are not on the host 
        unindex_keys = []
        for key in self.index.keys():
            if key not in file_dict.keys():
                unindex_keys.append(key)
        
        # compare file hashes
        #  delete files that don't need to be updated
        delete_keys = []
        latest_remote_modtime = 0.0
        latest_local_modtime = 0.0
        for filename in file_dict.keys():
            local_hash = file_dict[filename]
            remote_hash = self.index.get(filename, ("", ""))[0]
            local_modtime = os.path.getmtime(filename)

            try: 
                # set max local modtime
                if float(local_modtime) > latest_local_modtime:
                    latest_local_modtime = local_modtime
            except ValueError:
                pass
            remote_modtime = self.index.get(filename, ("", ""))[1]
            try:
                # set max remote modtime
                if float(remote_modtime) > latest_remote_modtime:
                    latest_remote_modtime = float(remote_modtime)
            except ValueError: 
                pass
            
            if local_hash == remote_hash or str(local_modtime) <= str(remote_modtime):
                delete_keys.append(filename)

        for key in delete_keys:
            del file_dict[key]

        # check the whole index for remote mod time
        for filename in self.index.keys():
            remote_modtime = self.index.get(filename, ("", ""))[1]
            if float(remote_modtime) > latest_remote_modtime:
                latest_remote_modtime = float(remote_modtime)

        # upload files that are different
        if latest_local_modtime > latest_remote_modtime:
            for filename in file_dict.keys():
                self.client.upload_file(Bucket=self.bucket_name, Key=f"{self.path}/{filename.replace(self.local_path, '')}", Filename=filename)
                print(f"COPYING {filename} to {self.path}/{filename}")
        else:
            for filename in file_dict.keys():
                print(filename)
                print(self.index)
                del self.index[filename]
                os.remove(filename)

        # delete from index only if files are being uploaded
        # todo: get a better algorithm
        if len(file_dict.keys()) > 0 and latest_local_modtime > latest_remote_modtime:
            for key in unindex_keys:
                print(f"UNINDEX {key}")
                del self.index[key]

        # update index
        index_str = ""
        for filename in file_dict.keys():
            self.index[filename] = (file_dict[filename], os.path.getmtime(filename))
        for filename in self.index.keys():
            index_str = index_str + f"{filename.replace(self.local_path, '')}:{self.index[filename][0]}:{self.index[filename][1]}\n"
        self.client.put_object(Bucket=self.bucket_name, Key=f"{self.path}/index", Body=bytes(index_str, "utf-8"))


    def parse_index(self, index_object):

        # if empty return empty index
        if index_object is None:
            self.index = {}
            return 
        
        # parse the body from s3 object data
        index_data = ""
        for batch in index_object['Body']:
            index_data = index_data + batch.decode()

        for line in index_data.split('\n'):
            if line != "":
                key, file_hash, modtime = line.split(':')
                self.index[self.local_path + key] = (file_hash, float(modtime))


    def restore(self):

        # get the list of folders and files to restore 
        folder_list, file_list = self.get_local_file_list()
        file_dict = {}
        for file_tup in file_list:
            file_dict[file_tup[0]] = file_tup[1]

        # get the previous index if there is any
        try:
            index_object = self.client.get_object(Bucket=self.bucket_name, Key=f"{self.path}/index")
        except:
            index_object = None
        
        # parse the data
        self.parse_index(index_object)

        # get folders and ensure they exist
        folder_list = []
        for key in self.index.keys():
            folder = "/".join(key.split('/')[:-1])
            if not os.path.exists(folder) and folder != '':
                os.makedirs(folder)

        # add in files from index that are not present in the list
        for key in self.index.keys():
            if key not in file_dict.keys():
                file_dict[key] = "0"
        
        # compare file hashes
        #  delete files that don't need to be updated
        delete_keys = []
        for filename in file_dict.keys():
            local_hash = file_dict[filename]
            remote_hash = self.index.get(filename, ("", ""))[0]
            try:
                local_modtime = os.path.getmtime(filename)
            except:
                local_modtime = "0"
            remote_modtime = self.index.get(filename, ("", ""))[1]
            if local_hash == remote_hash or str(local_modtime) >= str(remote_modtime):
                delete_keys.append(filename)

        for key in delete_keys:
            del file_dict[key]

        # restore files that are different or missing
        for filename in file_dict.keys():
            with open(filename, 'wb') as f:
                self.client.download_fileobj(self.bucket_name, f"{self.path}/{filename.replace(self.local_path, '')}", f)
                print(f"RESTORING {filename} from {self.path}/{filename.replace(self.local_path, '')}")

    def sync(self):
        self.save()
        self.restore()


class LocalSave(Save):

    def __init__(self, remote_path, local_path):

        super().__init__(os.path.expanduser(local_path))
        self.remote_path = os.path.expanduser(remote_path) + ("/" if remote_path[-1] != "/" else "")
        if not os.path.exists(self.remote_path):
            os.makedirs(self.remote_path)


    def save(self):

        # get the list of folders and files to copy 
        folder_list, file_list = self.get_local_file_list()
        file_dict = {}
        for file_tup in file_list:
            file_dict[file_tup[0]] = file_tup[1]

        self.create_folders(folder_list)

        # get the previous index if there
        try:
            index_object = open(f"{self.remote_path}/index", "r").readlines()
        except:
            index_object = None
        
        # parse the data
        self.parse_index(index_object)
        
        # compare file hashes
        #  delete files that don't need to be updated
        delete_keys = []
        for filename in file_dict.keys():
            local_hash = file_dict[filename]
            remote_hash = self.index.get(filename, ("", ""))[0]
            local_modtime = os.path.getmtime(filename)
            remote_modtime = self.index.get(filename, ("", ""))[1]
            if local_hash == remote_hash or local_modtime <= remote_modtime:
                delete_keys.append(filename)

        for key in delete_keys:
            del file_dict[key]
            

        # copy files that are different
        for filename in file_dict.keys():
            with open(filename,"rb") as fin,open(f"{self.remote_path}{filename.replace(self.local_path, '')}","wb") as fout:
                print(f"COPYING {filename} to {self.remote_path}{filename.replace(self.local_path, '')}")                
                data = fin.read(1024)
                while data:
                    fout.write(data)
                    data = fin.read(1024)

        # update index
        index_str = ""
        for filename in file_dict.keys():
            self.index[filename] = (file_dict[filename], os.path.getmtime(filename))
        for filename in self.index.keys():
            index_str = index_str + f"{filename.replace(self.local_path, '')}:{self.index[filename][0]}:{self.index[filename][1]}\n"
        with open(f"{self.remote_path}/index", "w") as f:
            f.write(index_str)


    def parse_index(self, index_object):

        # if empty return empty index
        if index_object is None:
            self.index = {}
            return 
        
        # parse the body from s3 object data
        for line in index_object:
            if line != "":
                key, file_hash, modtime = line.split(':')
                self.index[self.local_path + key] = (file_hash, float(modtime))

    
    def create_folders(self, folder_list):
        for folder in folder_list:
            folder = self.remote_path + folder.replace(self.local_path, '')
            if not os.path.exists(folder):
                print(f"CREATING FOLDER {folder}")
                os.makedirs(folder)
        
    def restore(self):

        # get the list of folders and files to restore 
        folder_list, file_list = self.get_local_file_list()
        file_dict = {}
        for file_tup in file_list:
            file_dict[file_tup[0]] = file_tup[1]

        # get the previous index if there
        try:
            index_object = open(f"{self.remote_path}/index", "r").readlines()
        except:
            index_object = None
        
        # parse the data
        self.parse_index(index_object)

        # get folders and ensure they exist
        folder_list = []
        for key in self.index.keys():
            folder = "/".join(key.split('/')[:-1])
            if not os.path.exists(folder) and folder != '':
                os.makedirs(folder)

        # add in files from index that are not present in the list
        for key in self.index.keys():
            if key not in file_dict.keys():
                file_dict[key] = "0"
        
        # compare file hashes
        #  delete files that don't need to be updated
        delete_keys = []
        for filename in file_dict.keys():
            local_hash = file_dict[filename]
            remote_hash = self.index.get(filename, ("", ""))[0]
            try:
                local_modtime = os.path.getmtime(filename)
            except:
                local_modtime = "0"
            remote_modtime = self.index.get(filename, ("", ""))[1]
            if local_hash == remote_hash or local_modtime >= remote_modtime:
                delete_keys.append(filename)

        for key in delete_keys:
            del file_dict[key]

        # upload files that are different
        for filename in file_dict.keys():
            with open(f"{self.remote_path}{filename.replace(self.local_path, '')}","rb") as fin,open(filename,"wb") as fout:
                print(f"RESTORING {filename} from {self.remote_path}{filename.replace(self.local_path, '')}")
                data = fin.read(1024)
                while data:
                    fout.write(data)
                    data = fin.read(1024)

    def sync(self):
        self.save()
        self.restore()