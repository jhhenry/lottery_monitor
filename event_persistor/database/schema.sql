
/* A table records the details of lottery events:
id (key)
txn(indexed)
event type,
timestamp,
event details(json):
    //lottery, rs1, rs2, dest, issue_time, token_address, faceValue, power, token_address
    lottery, rs1, rs2, token_address, faceValue, issuer, winner, sender
 */
create table events(
    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    txn CHAR(66) NOT NULL,
    blockNumber SMALLINT UNSIGNED,
    event_capture_time TIMESTAMP(3) NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    event_info JSON NOT NULL,
    PRIMARY KEY (id)
);
/* A table records rs1 & rs2 of a winning lottery. 
id (foreign key)
signature
rs1
rs2
paying_party
beneficiary
sender
*/
create table redeemedInfo(
    id SMALLINT UNSIGNED NOT NULL REFERENCES events(id),
    lottery_sig CHAR(132) NOT NULL KEY,
    rs1 VARCHAR(66) NOT NULL,
    rs2 VARCHAR(66) NOT NULL,
    payer CHAR(42) NOT NULL,
    beneficiary CHAR(42) NOT NULL,
    sender CHAR(42) NOT NULL
);

