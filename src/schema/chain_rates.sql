CREATE TABLE reporting.chain_rates (
    valid_from timestamp without time zone ENCODE raw,
    valid_to timestamp without time zone ENCODE az64,
    days_supply_from decimal(5,0), 
    days_supply_to decimal(5,0),
    chain_name character varying(512) NOT NULL ENCODE raw distkey,
    state_abbreviation character varying(2) ENCODE lzo,
    drug_type character varying(512) ENCODE lzo,
    target_rate numeric(4,2) ENCODE az64,
    bin_number varchar(6) default '019876',
    partner varchar(512) default '',
    PRIMARY KEY (chain_name)
)
DISTSTYLE KEY
SORTKEY ( valid_from, bin_number, chain_name );

insert into reporting.chain_rates values('2021-01-01','2300-01-01',0,99999, 'GIANT EAGLE PHARMACY','','generic',87,'019876','');
insert into reporting.chain_rates values('2021-01-01','2300-01-01',0,99999, 'GIANT EAGLE PHARMACY','','brand',87,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','',  'generic',72,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','',  'brand',13,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','MA','brand',12.5,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','AK','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','HI','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','AS','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','FM','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','GU','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','MH','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','MP','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','PR','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','PW','brand',10,'019876','');
insert into reporting.chain_rates values('2021-05-01','2300-01-01',0,99999, 'walgreens','VI','brand',10,'019876','');
insert into reporting.chain_rates values('2021-04-01','2300-01-01',0,99999, 'cvs','','generic',71,'019876','');
insert into reporting.chain_rates values('2021-04-01','2021-09-30',0,99999, 'cvs','','OTC generic',71,'019876','');
insert into reporting.chain_rates values('2021-10-01','2300-01-01',0,99999, 'cvs','','OTC generic',63,'019876','');
insert into reporting.chain_rates values('2021-04-01','2021-12-31',0,99999, 'cvs','','brand',12.55,'019876','');
insert into reporting.chain_rates values('2022-01-01','2300-01-01',0,99999, 'cvs','','brand',12.3,'019876','');
insert into reporting.chain_rates values('2021-04-01','2300-01-01',0,99999, 'cvs','','OTC brand',12.55,'019876','');
--CVS TPDT 019901
insert into reporting.chain_rates values('2022-02-01','2300-01-01',0,99999, 'cvs','','generic',77,'019901','');
insert into reporting.chain_rates values('2022-02-01','2300-01-01',0,99999, 'cvs','','OTC generic',63,'019901','');
insert into reporting.chain_rates values('2022-02-01','2300-01-01',0,89,    'cvs','','brand',16,'019901','');
insert into reporting.chain_rates values('2022-02-01','2300-01-01',90,99999,'cvs','','brand',18,'019901','');
insert into reporting.chain_rates values('2022-02-01','2300-01-01',0,99999, 'cvs','','OTC brand',10,'019901','');
insert into reporting.chain_rates values('2022-02-01','2300-01-01',0,99999, 'cvs','','Specialty generic',89,'019901','');
--CVS partner WebMd
insert into reporting.chain_rates values('2023-01-21','2300-01-01',0,99999, 'cvs','','generic',75,'019876','webmd');
insert into reporting.chain_rates values('2023-01-21','2300-01-01',0,99999, 'cvs','','OTC generic',63,'019876','webmd');
insert into reporting.chain_rates values('2023-01-21','2300-01-01',0,99999, 'cvs','','brand',12.55,'019876','webmd');
insert into reporting.chain_rates values('2023-01-21','2300-01-01',0,99999, 'cvs','','OTC brand',12.55,'019876','webmd');

