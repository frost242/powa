package Powa::System;

# This program is open source, licensed under the PostgreSQL Licence.
# For license terms, see the LICENSE file.

use Mojo::Base 'Mojolicious::Controller';

use Data::Dumper;
use Digest::SHA qw(sha256_hex);
use Mojo::ByteStream 'b';
use Powa::Beautify;

use Mojo::Log;

sub all {
    my $self = shift;
    my $base_timestamp = undef;

    $base_timestamp = $self->config->{base_timestamp} if ( defined $self->config->{base_timestamp} );

    $self->stash( 'base_timestamp' => $base_timestamp );
	$self->stash( subtitle => 'System activity' );

    $self->stash( 'device_names' => 'sda' );

    $self->render();
}

sub cpu {
    my $self = shift;
    my $base_timestamp = undef;

    $base_timestamp = $self->config->{base_timestamp} if ( defined $self->config->{base_timestamp} );

    $self->stash( 'base_timestamp' => $base_timestamp );
	$self->stash( subtitle => 'CPU usage' );

    $self->render();
}

sub cpudata_agg {
	my $self = shift;
	my $dbh  = $self->database();
    my $from = $self->param("from");
    my $to   = $self->param("to");
	my $json = Mojo::JSON->new;
	my $sql;

    $from = substr $from, 0, -3;
    $to = substr $to, 0, -3;

	$sql = $dbh->prepare("SELECT (extract(epoch FROM ts)*1000)::bigint AS ts,
                   round(cpuuser::numeric, 2) AS cpuuser, round(cpunice::numeric, 2) AS cpunice,
                   round(cpusystem::numeric, 2) AS cpusystem,
                   round(cpuiowait::numeric, 2) AS cpuiowait, round(cpuirq::numeric, 2) AS cpuirq,
                   round(cpusoftirq::numeric, 2) AS cpusoftirq, round(cpusteal::numeric, 2) AS cpusteal
              FROM powa_proctab_get_cpu_statdata_sample(to_timestamp(?), to_timestamp(?), 300)
");
	$sql->execute($from, $to);

    my $data = [];
    my $series = {};

    while ( my @tab = $sql->fetchrow_array() ) {
		push @{$series->{'cpuuser'}},  [ 0 + $tab[0], 0.0 + $tab[1] ];
		push @{$series->{'cpunice'}},   [ 0 + $tab[0], 0.0 + $tab[2] ];
		push @{$series->{'cpusystem'}},   [ 0 + $tab[0], 0.0 + $tab[3] ];
		push @{$series->{'cpuiowait'}},   [ 0 + $tab[0], 0.0 + $tab[4] ];
		push @{$series->{'cpuirq'}},   [ 0 + $tab[0], 0.0 + $tab[5] ];
		push @{$series->{'cpusoftirq'}},   [ 0 + $tab[0], 0.0 + $tab[6] ];
		push @{$series->{'cpusteal'}},   [ 0 + $tab[0], 0.0 + $tab[7] ];
	}
    $sql->finish();

	$dbh->disconnect();

	push @{$data}, { data => $series->{'cpuuser'}, label => "user" };
	push @{$data}, { data => $series->{'cpunice'}, label => "nice" };
	push @{$data}, { data => $series->{'cpusystem'}, label => "system" };
	push @{$data}, { data => $series->{'cpuiowait'}, label => "waitio" };
	push @{$data}, { data => $series->{'cpuirq'}, label => "irq" };
	push @{$data}, { data => $series->{'cpusoftirq'}, label => "softirq" };
	push @{$data}, { data => $series->{'cpusteal'}, label => "steal" };

	my $properties = {};
    $properties->{legend}{show} = $json->false;
    $properties->{legend}{position} = "ne";

	$properties->{yaxis}{unit} = '%';
	$properties->{lines}{stacked} = $json->true;
	$properties->{lines}{fill} = $json->true;

	$properties->{title} = "POWA : CPU Usage";
    $properties->{yaxis}{autoscale} = $json->true;
    $properties->{yaxis}{autoscaleMargin} = 0.2;

    $self->render( json => {
		series		=> $data,
		properties	=> $properties
    } );
}

sub mem {
    my $self = shift;
    my $base_timestamp = undef;

    $base_timestamp = $self->config->{base_timestamp} if ( defined $self->config->{base_timestamp} );

    $self->stash( 'base_timestamp' => $base_timestamp );
	$self->stash( subtitle => 'Memory usage' );

    $self->render();
}


sub memdata_agg {
	my $self = shift;
	my $dbh  = $self->database();
    my $id     = $self->param("id");
    my $from = $self->param("from");
    my $to   = $self->param("to");
	my $json = Mojo::JSON->new;
	my $sql;

    $from = substr $from, 0, -3;
    $to = substr $to, 0, -3;

	$sql = $dbh->prepare("SELECT (extract(epoch FROM ts)*1000)::bigint AS ts,
                   memused, memfree, memshared, membuffers, memcached,
                   swapused, swapfree, swapcached
              FROM powa_proctab_get_mem_statdata_sample(to_timestamp(?), to_timestamp(?), 300)
");
	$sql->execute($from, $to);

    my $data = [];
    my $series = {};

    while ( my @tab = $sql->fetchrow_array() ) {
        if ( $id eq "mem" ) {
    		push @{$series->{'memused'}},  [ 0 + $tab[0], 0.0 + $tab[1] ];
	    	push @{$series->{'memfree'}},   [ 0 + $tab[0], 0.0 + $tab[2] ];
		    push @{$series->{'memshared'}},   [ 0 + $tab[0], 0.0 + $tab[3] ];
    		push @{$series->{'membuffers'}},   [ 0 + $tab[0], 0.0 + $tab[4] ];
	    	push @{$series->{'memcached'}},   [ 0 + $tab[0], 0.0 + $tab[5] ];
        } else {
		    push @{$series->{'swapused'}},   [ 0 + $tab[0], 0.0 + $tab[6] ];
    		push @{$series->{'swapfree'}},   [ 0 + $tab[0], 0.0 + $tab[7] ];
	    	push @{$series->{'swapcached'}},   [ 0 + $tab[0], 0.0 + $tab[8] ];
        }
	}
    $sql->finish();

	$dbh->disconnect();

    if ( $id eq "mem" ) {
        push @{$data}, { data => $series->{'memused'}, label => "used" };
        push @{$data}, { data => $series->{'memfree'}, label => "free" };
        push @{$data}, { data => $series->{'memshared'}, label => "shared" };
        push @{$data}, { data => $series->{'membuffers'}, label => "buffers" };
        push @{$data}, { data => $series->{'memcached'}, label => "cached" };
    } else {
        push @{$data}, { data => $series->{'swapused'}, label => "swapused" };
        push @{$data}, { data => $series->{'swapfree'}, label => "swapfree" };
        push @{$data}, { data => $series->{'swapcached'}, label => "swapcached" };
    }
	my $properties = {};
    $properties->{legend}{show} = $json->false;
    $properties->{legend}{position} = "ne";

	$properties->{yaxis}{unit} = 'B';
	$properties->{lines}{stacked} = $json->true;
	$properties->{lines}{fill} = $json->true;

	$properties->{title} = "POWA : Memory Usage";
    $properties->{yaxis}{autoscale} = $json->true;
    $properties->{yaxis}{autoscaleMargin} = 0.2;

    $self->render( json => {
		series		=> $data,
		properties	=> $properties
    } );
}


sub load {
    my $self = shift;
    my $base_timestamp = undef;

    $base_timestamp = $self->config->{base_timestamp} if ( defined $self->config->{base_timestamp} );

    $self->stash( 'base_timestamp' => $base_timestamp );
	$self->stash( subtitle => 'Load average' );

    $self->render();
}

sub loaddata_agg {
	my $self = shift;
	my $dbh  = $self->database();
    my $from = $self->param("from");
    my $to   = $self->param("to");
	my $json = Mojo::JSON->new;
	my $sql;

    $from = substr $from, 0, -3;
    $to = substr $to, 0, -3;

	$sql = $dbh->prepare("SELECT (extract(epoch FROM ts)*1000)::bigint AS ts,
                   load1, load5, load15
              FROM powa_proctab_get_load_statdata_sample(to_timestamp(?), to_timestamp(?), 300)
");
	$sql->execute($from, $to);

    my $data = [];
    my $series = {};

    while ( my @tab = $sql->fetchrow_array() ) {
		push @{$series->{'load1'}},  [ 0 + $tab[0], 0.0 + $tab[1] ];
		push @{$series->{'load5'}},   [ 0 + $tab[0], 0.0 + $tab[2] ];
		push @{$series->{'load15'}},   [ 0 + $tab[0], 0.0 + $tab[3] ];
	}
    $sql->finish();

	$dbh->disconnect();

	push @{$data}, { data => $series->{'load1'}, label => "load1" };
	push @{$data}, { data => $series->{'load5'}, label => "load5" };
	push @{$data}, { data => $series->{'load15'}, label => "load15" };

	my $properties = {};
    $properties->{legend}{show} = $json->false;
    $properties->{legend}{position} = "ne";

	$properties->{yaxis}{unit} = '';
	$properties->{lines}{stacked} = $json->false;
	$properties->{lines}{fill} = $json->true;

	$properties->{title} = "POWA : Load Average";
    $properties->{yaxis}{autoscale} = $json->true;
    $properties->{yaxis}{autoscaleMargin} = 0.2;

    $self->render( json => {
		series		=> $data,
		properties	=> $properties
    } );
}

sub disk {
    my $self = shift;
    my $base_timestamp = undef;

    $base_timestamp = $self->config->{base_timestamp} if ( defined $self->config->{base_timestamp} );

    $self->stash( 'base_timestamp' => $base_timestamp );
	$self->stash( subtitle => 'Disk usage' );

    $self->render();
}


sub diskdata_agg {
	my $self   = shift;
	my $dbh    = $self->database();
    my $device = $self->param("device");
    my $id     = $self->param("id");
    my $from   = $self->param("from");
    my $to     = $self->param("to");
	my $json   = Mojo::JSON->new;
	my $sql;

    $device = "sda";

    $from = substr $from, 0, -3;
    $to = substr $to, 0, -3;

	$sql = $dbh->prepare("SELECT (extract(epoch FROM ts)*1000)::bigint AS ts,
                   devname,
                   reads_completed, reads_merged, sectors_read, readtime,
                   writes_completed, writes_merged, sectors_written, writetime,
                   current_io, iotime, totaliotime
              FROM powa_proctab_get_disk_statdata_sample(to_timestamp(?), to_timestamp(?), 300)
             WHERE devname = ?
");
	$sql->execute($from, $to, $device);

    my $data = [];
    my $series = {};

    while ( my @tab = $sql->fetchrow_array() ) {
        if ( $id eq "reads" ) {
            push @{$series->{'reads_completed'}},   [ 0 + $tab[0], 0.0 + $tab[2] ];
            push @{$series->{'reads_merged'}},   [ 0 + $tab[0], 0.0 + $tab[3] ];
        } elsif ( $id eq "read_time" ) { 
            push @{$series->{'readtime'}},   [ 0 + $tab[0], 0.0 + $tab[5] ];
        } elsif ( $id eq "writes" ) {
            push @{$series->{'writes_completed'}},   [ 0 + $tab[0], 0.0 + $tab[6] ];
            push @{$series->{'writes_merged'}},   [ 0 + $tab[0], 0.0 + $tab[7] ];
        } elsif ( $id eq "write_time" ) { 
            push @{$series->{'writetime'}},   [ 0 + $tab[0], 0.0 + $tab[9] ];
        } elsif ( $id eq "total_io_time" ) { 
            push @{$series->{'totaliotime,,'}},   [ 0 + $tab[0], 0.0 + $tab[12] ];
        } elsif ( $id eq "total_read" ) {
            push @{$series->{'sectors_read'}},   [ 0 + $tab[0], 0.0 + $tab[4] ];
        } elsif ( $id eq "total_written" ) {
            push @{$series->{'sectors_written'}},   [ 0 + $tab[0], 0.0 + $tab[8] ];
        }
	}
    $sql->finish();

	$dbh->disconnect();

	my $properties = {};
    $properties->{lines}{stacked} = $json->false;
   	$properties->{lines}{fill} = $json->false;

    if ( $id eq "reads" ) {
    	push @{$data}, { data => $series->{'reads_completed'}, label => "Completed reads" };
    	push @{$data}, { data => $series->{'reads_merged'}, label => "Merged reads" };
	    $properties->{yaxis}{unit} = 'iop';
      	$properties->{lines}{fill} = $json->true;
    } elsif ( $id eq "writes" ) { 
    	push @{$data}, { data => $series->{'writes_completed'}, label => "Completed writes" };
    	push @{$data}, { data => $series->{'writes_merged'}, label => "Merged writes" };
	    $properties->{yaxis}{unit} = 'iop';
    	$properties->{lines}{fill} = $json->true;
    } elsif ( $id eq "total_read" ) {
        # sector size can be found in /sys/block/$dev/queue/hw_sector_size
    	push @{$data}, { data => $series->{'sectors_read'}, label => "Sectors read" };
	    $properties->{yaxis}{unit} = 'sectors';
    	$properties->{lines}{fill} = $json->true;
    } elsif ( $id eq "total_written" ) {
        # sector size can be found in /sys/block/$dev/queue/hw_sector_size
    	push @{$data}, { data => $series->{'sectors_written'}, label => "Sectors written" };
	    $properties->{yaxis}{unit} = 'sectors';
    	$properties->{lines}{fill} = $json->true;
    } elsif ( $id eq "read_time" ) {
    	push @{$data}, { data => $series->{'readtime'}, label => "Read time" };
	    $properties->{yaxis}{unit} = 'ms';
    } elsif ( $id eq "write_time" ) {
    	push @{$data}, { data => $series->{'writetime'}, label => "Write time" };
	    $properties->{yaxis}{unit} = 'ms';
    }

    $properties->{legend}{show} = $json->false;
    $properties->{legend}{position} = "ne";
	$properties->{title} = "POWA : Disk usage";
    $properties->{yaxis}{autoscale} = $json->true;
    $properties->{yaxis}{autoscaleMargin} = 0.2;

    $self->render( json => {
		series		=> $data,
		properties	=> $properties
    } );
}

1;
