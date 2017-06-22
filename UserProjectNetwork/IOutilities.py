class IOutilities(object):

    def __init__(self):
        pass


    @staticmethod
    def printDict(my_dict, num_elements):
        print('the size of the dict is {}'.format(len(my_dict)))
        print('now printing first {} key value pairs of dict passed in'.format(num_elements))
        count = 0
        for key, value in my_dict.items():
            print("key is {} and corresponding value is {}".format(key, value))
            count += 1
            if count > num_elements:
                break

    @staticmethod
    def printDicttoFile(my_dict, filename):
        f = open(filename, 'w')
        for key, value in my_dict.items():
            f.write("{},{}\n".format(key, value))
