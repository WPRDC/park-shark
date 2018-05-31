from parameters.remote_parameters import webhook_url

def send_to_slack(message,username=None,channel=None,icon=None):
    """This script sends the given message to a particular channel on
    Slack, as configured by the webhook_url. Note that this shouldn't 
    be heavily used (e.g., for reporting every error a script 
    encounters) as API limits are a consideration. This script IS 
    suitable for running when a script-terminating exception is caught, 
    so that you can report the irregular termination of an ETL script."""

    import os, re, json, requests
    import socket
    IP_address = socket.gethostbyname(socket.gethostname())
    hostname = re.sub(".local","",socket.gethostname())
    name_of_current_script = os.path.basename(__file__)

    caboose = "(Sent from {} running on a computer called {} at {}.)".format(name_of_current_script, hostname, IP_address)
    # Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/
    slack_data = {'text': message + " " + caboose}
    slack_data['username'] = 'TACHYON'
    if username is not None:
        slack_data['username'] = username
    #To send this as a direct message instead, use the following line. 
    if channel is not None:
        slack_data['channel'] = channel
    if icon is not None:
        slack_data['icon_emoji'] = icon #':coffin:' #':tophat:' # ':satellite_antenna:'
    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to Slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )

if __name__ == '__main__':
    msg = "No sir, away! A papaya war is on!"
    send_to_slack(msg)
