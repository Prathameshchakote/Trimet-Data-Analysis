def validate_records(data):
    #print(f"The data type of the provided data is: {type(data)}")
    if isinstance(data, dict):
        data = [data]  # Convert single dictionary to list of one dictionary
    
    required_fields = ["vehicle_number", "route_number", "direction", "service_key"]
    
    for record in data:
        missing_fields = [field for field in required_fields if field not in record]
        if missing_fields:
            print(f"Missing fields in record: {record}\nMissing fields: {missing_fields}")
            return False
    return True

def check_numeric_field(data):
    if isinstance(data, dict):
        data = [data]
    numeric_fields = ["vehicle_number", "route_number"]
    
    for record in data:
        for field in numeric_fields:
            try:
                # Try to convert the field to a float
                value = float(record[field])
                # If conversion succeeds, check if it's an integer or float
                if not isinstance(value, (int, float)):
                    print("Field {field} is not numeric in record: ")
                    return False
            except ValueError:
                # If conversion fails, the field is not numeric
                print(f"Field {field} is not numeric in record")
                return False
    return True

def validate_direction(data):
    if isinstance(data, dict):
        data = [data]
        
    for record in data:
        if not (isinstance(record["direction"], str) and len(record["direction"]) == 1)and (record["direction"]=="0" or record["direction"] =="1"):
            print(f"Field direction is not a single character in record")
            return False
    return True

def validate_service_key(data):
    if isinstance(data, dict):
        data = [data]
        
    for record in data:
        service_key = record.get("service_key")
        if not (isinstance(service_key, str) and service_key):
            print(f"Field service_key is not a valid non-empty string in record: {service_key}")
            return False
    return True

def validate_vehicle_id(data):
    vehicle_ids = [
        '3940', '3137', '3513', '3905', '3220', '3415', '3157', '3732', '3543', '4035', '3924', '3540', '3227', 
        '4237', '4039', '3247', '3166', '3209', '3722', '3950', '3925', '3512', '3956', '3560', '2909', '2933', 
        '3235', '3261', '3556', '4050', '3241', '3749', '3154', '3959', '3149', '3143', '3237', '3017', '2910', 
        '3511', '3571', '3954', '4516', '3055', '3625', '3907', '4518', '3946', '3729', '3634', '3952', '3918', 
        '3527', '3728', '3410', '3719', '3254', '3516', '3508', '3928', '3028', '3707', '4525', '3549', '3741', 
        '4001', '3529', '3915', '3322', '3040', '2926', '3510', '3943', '3957', '3562', '3702', '3039', '3648', 
        '3909', '3505', '3226', '3134', '3216', '3120', '3020', '3620', '4028', '2908', '3731', '3320', '3401', 
        '3617', '3101', '3921', '4522', '2901', '3727', '3605', '3746', '4210'
    ]
    
    if isinstance(data, dict):
        data = [data]
    #print(data)

    for record in data:
        
        vehicle_id = record.get("vehicle_number")
        #print(f"vehicle_id: {vehicle_id}, type: {type(vehicle_id)}") 
        if vehicle_id not in vehicle_ids:
            print(f"Field vehicle_id is not in the predefined set in record: {vehicle_id}")
            return False
    return True

