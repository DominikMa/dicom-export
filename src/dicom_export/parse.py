def parse_response(response: str):
    data = []

    uid = 0
    std_split = response.split('\n')

    for line in std_split:
        if line.startswith('I: ---------------------------'):
            data.append({})
            data[-1]['uid'] = {}
            data[-1]['uid']['tag'] = 0
            data[-1]['uid']['value'] = uid
            data[-1]['uid']['label'] = 'uid'
            uid += 1

        elif line.startswith('I: '):
            lineSplit = line.split()
            if len(lineSplit) >= 8 and re.search('\((.*?)\)', lineSplit[1]) != None:
                # extract DICOM tag
                tag = re.search('\((.*?)\)', lineSplit[1]).group(0)[1:-1].strip().replace('\x00', '')

                # extract value
                value = re.search('\[(.*?)\]', line)
                if value != None:
                    value = value.group(0)[1:-1].strip().replace('\x00', '')
                else:
                    value = 'no value provided for %s' % tag

                # extract label
                label = lineSplit[-1].strip()

                data[-1][label] = {}
                data[-1][label]['tag'] = tag
                data[-1][label]['value'] = value
                data[-1][label]['label'] = label
            else:
                # Only append the line output for the echo command
                if type(self).__name__ == 'Echo': data.append(line)

    return data


def check_response(response):
    std_split = response.split('\n')
    info_count = 0
    error_count = 0
    for line in std_split:
        if line.startswith('I: '):
            info_count += 1
        elif line.startswith('E: ') or 'error' in line.lower():
            error_count += 1

    status = 'error'
    if error_count == 0:
        status = 'success'

    return status


def format_response(stdout, args, returncode):
    std = stdout.decode('utf-8', 'slashescape')
    response = {
        'status': 'success',
        'data': '',
        'command': args,
        'returncode': returncode
    }

    status = check_response(std)

    if status == 'error':
        response['status'] = 'error'
        response['data'] = std
    else:
        response['status'] = 'success'
        response['data'] = parse_response(std)

    return response


if __name__ == '__main__':
    print()