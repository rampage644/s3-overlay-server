[servers]
%{ for dns in ip_addresses ~}
${ dns } ansible_user=ec2-user ansible_ssh_private_key_file=~/.ssh/id_rsa
%{ endfor ~}