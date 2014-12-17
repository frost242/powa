package Powa::System;

# This program is open source, licensed under the PostgreSQL Licence.
# For license terms, see the LICENSE file.

use Mojo::Base 'Mojolicious::Controller';

use Data::Dumper;
use Digest::SHA qw(sha256_hex);
use Mojo::ByteStream 'b';
use Powa::Beautify;

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
	my $tmp;
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
    my $from = $self->param("from");
    my $to   = $self->param("to");
	my $json = Mojo::JSON->new;
	my $tmp;
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
		push @{$series->{'memused'}},  [ 0 + $tab[0], 0.0 + $tab[1] ];
		push @{$series->{'memfree'}},   [ 0 + $tab[0], 0.0 + $tab[2] ];
		push @{$series->{'memshared'}},   [ 0 + $tab[0], 0.0 + $tab[3] ];
		push @{$series->{'membuffers'}},   [ 0 + $tab[0], 0.0 + $tab[4] ];
		push @{$series->{'memcached'}},   [ 0 + $tab[0], 0.0 + $tab[5] ];
		push @{$series->{'swapused'}},   [ 0 + $tab[0], 0.0 + $tab[6] ];
		push @{$series->{'swapfree'}},   [ 0 + $tab[0], 0.0 + $tab[7] ];
		push @{$series->{'swapcached'}},   [ 0 + $tab[0], 0.0 + $tab[8] ];
	}
    $sql->finish();

	$dbh->disconnect();

	push @{$data}, { data => $series->{'memused'}, label => "used" };
	push @{$data}, { data => $series->{'memfree'}, label => "free" };
	push @{$data}, { data => $series->{'memshared'}, label => "shared" };
	push @{$data}, { data => $series->{'membuffers'}, label => "buffers" };
	push @{$data}, { data => $series->{'memcached'}, label => "cached" };
	push @{$data}, { data => $series->{'swapused'}, label => "swapused" };
	push @{$data}, { data => $series->{'swapfree'}, label => "swapfree" };
	push @{$data}, { data => $series->{'swapcached'}, label => "swapcached" };

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
	my $tmp;
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
	$properties->{lines}{stacked} = $json->true;
	$properties->{lines}{fill} = $json->true;

	$properties->{title} = "POWA : Load Average";
    $properties->{yaxis}{autoscale} = $json->true;
    $properties->{yaxis}{autoscaleMargin} = 0.2;

    $self->render( json => {
		series		=> $data,
		properties	=> $properties
    } );
}


1;
