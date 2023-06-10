def generate_query_list(client_data):
        """
        This method returns a set of customised queries that needs to be parsed via NewsAPI
        Input:
            Client_data : A dataframe with all the required data from client such as supplier,supplier location, raw material, etc
        Output:
            query_list: A list of templated queries using the input client data and some external sub terms
        """
        supplier_query_list=[]
        port_query_list=[]
        plant_location_query_list=[]
        product_query_list=[]
        for index,row in client_data.iterrows():
            supplier_query = "("+row['Supplier Name']+") OR (("+row["Raw material/Commodity Provided"]+' OR '+row['commodity']+") AND "+row["Supplier Country"]+") OR (("+row["Raw material/Commodity Provided"]+' OR '+row['commodity']+") AND "+row["Supplier City/State"]+")"
            supplier_query_list.append(supplier_query)
            product_query = "("+str(row['Products manufactured that utilize the given raw material'])+") AND (Effects OR good OR bad)"
            product_query_list.append(product_query)
            if str(row['Source Port, in case of import'])!='nan':
                port_query = "("+str(row['Source Port, in case of import'])+") AND (Natural Calamities OR Disasters)"
                port_query_list.append(port_query)
            if str(row['Plant Location'])!='nan':
                plant_location_query = "("+str(row['Plant Location'])+") AND (Natural Calamities OR Disasters)"
                plant_location_query_list.append(plant_location_query)
        query_list=supplier_query_list+product_query_list+port_query_list+plant_location_query_list
        return query_list