from collections import defaultdict

from numba import jit

@jit(cache=True, nogil=True)
def bbgloop_numba(fieldData, ticker):

    data = defaultdict(dict)

    # FASTER avoid calling getValue/getElement methods in blpapi, very slow, better to cache variables
    # for i in range(fieldData.numValues()):
    #     mini_field_data = fieldData.getValue(i)
    #     date = mini_field_data.getElement(0).getValue()
    #
    #     for j in range(1, mini_field_data.numElements()):
    #         field_value = mini_field_data.getElement(j)
    #
    #         data[(str(field_value.name()), ticker)][date] = field_value.getValue()

    temp = fieldData.numValues()

    for i in range(temp):
        mini_field_data = fieldData.getValue(i)
        date = mini_field_data.getElement(0).getValue()

        temp1 =  mini_field_data.numElements()
        for j in range(1, temp1):
            field_value = mini_field_data.getElement(j)

            data[(str(field_value.name()), ticker)][date] = field_value.getValue()

    return data
