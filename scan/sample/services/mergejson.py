import json


def merge_json_files(file_paths):
    merged_contents = []

    for file_path in file_paths:
        with open(file_path, 'r', encoding='utf-8') as file_in:
            merged_contents.extend(json.load(file_in))

    with open('aaservices.json', 'w', encoding='utf-8') as file_out:
        json.dump(merged_contents, file_out, indent=2)


paths = [
    'appsync.json',
    'backup.json',
    'config.json',
    'directconnect.json',
    'elb.json',
    'elbv2.json',
    'events.json',
    'lambda.json',
    'sqs.json',
    'secretsmanager.json',
    'securityhub.json',
    'servicecatalog-appregistry.json',
    'servicecatalog.json',
    'xray.json',
    'athena.json',
    'cloudwatch.json',
    'connect.json',
    'connect-contact-lens.json',
    'dynamodb.json',
    'ec2.json',
    'glacier.json',
    'guardduty.json',
    'kinesis.json',
    'kinesisanalytics.json',
    'kinesisanalyticsv2.json',
    'kinesisvideo.json',
    'kinesis-video-archived-media.json',
    'kinesis-video-media.json',
    'kinesis-video-signaling.json',
    'kinesis-video-webrtc-storage.json',
    'firehose.json',
    'pinpoint.json',
    'pinpoint-email.json',
    'pinpoint-sms-voice-v2.json',
    'pinpoint-sms-voice.json',
    's3.json',
    'stepfunctions.json',
    'codebuild.json',
    'connectcampaigns.json',
    'connectcases.json',
    'connectparticipant.json',
    'kms.json',
    'waf.json',
    'wafv2.json',
    'waf-regional.json'
]

merge_json_files(paths)
