#!/usr/bin/env perl

use strict;
use warnings;
use utf8;

use IO::Handle ();
use JSON::PP ();
use Scalar::Util qw(looks_like_number);

use ProductOpener::Config qw(:all);
use ProductOpener::DataQualityFood qw(is_european_product);
use ProductOpener::Products qw(analyze_and_enrich_product_data normalize_product_data);
use ProductOpener::Tags qw(get_all_tags_having_property get_inherited_property_from_categories_tags);

my $json = JSON::PP->new->utf8(1);

die "Usage: off_runtime.pl\n" if @ARGV;

my $input_fh = *STDIN;
my $output_fh = *STDOUT;
$output_fh->autoflush(1);

sub _snapshot_number {
	my ($value) = @_;
	return undef if not defined $value;
	return undef if ref($value);
	return looks_like_number($value) ? sprintf("%.17g", $value) : $value;
}

sub _snapshot_array {
	my ($value) = @_;
	return [] if ref($value) ne 'ARRAY';
	return $value;
}

sub _snapshot_hash {
	my ($value) = @_;
	return {} if ref($value) ne 'HASH';
	return $value;
}

sub _enriched_snapshot_for {
	my ($product_ref, $helper_state_ref) = @_;

	return {
		product => {
			code => $product_ref->{code},
			lc => $product_ref->{lc},
			lang => $product_ref->{lang},
			created_t => _snapshot_number($product_ref->{created_t}),
			packagings => $product_ref->{packagings},
			product_name => $product_ref->{product_name},
			quantity => $product_ref->{quantity},
			product_quantity => _snapshot_number($product_ref->{product_quantity}),
			serving_size => $product_ref->{serving_size},
			serving_quantity => _snapshot_number($product_ref->{serving_quantity}),
			brands => $product_ref->{brands},
			categories => $product_ref->{categories},
			labels => $product_ref->{labels},
			emb_codes => $product_ref->{emb_codes},
			ingredients_text => $product_ref->{ingredients_text},
			ingredients => _snapshot_array($product_ref->{ingredients}),
			ingredients_percent_analysis => _snapshot_number($product_ref->{ingredients_percent_analysis}),
			ingredients_with_specified_percent_n => _snapshot_number($product_ref->{ingredients_with_specified_percent_n}),
			ingredients_with_unspecified_percent_n => _snapshot_number($product_ref->{ingredients_with_unspecified_percent_n}),
			ingredients_with_specified_percent_sum => _snapshot_number($product_ref->{ingredients_with_specified_percent_sum}),
			ingredients_with_unspecified_percent_sum => _snapshot_number($product_ref->{ingredients_with_unspecified_percent_sum}),
			nutriscore_grade => $product_ref->{nutriscore_grade},
			nutriscore_grade_producer => $product_ref->{nutriscore_grade_producer},
			nutriscore_score => _snapshot_number($product_ref->{nutriscore_score}),
			categories_tags => _snapshot_array($product_ref->{categories_tags}),
			labels_tags => _snapshot_array($product_ref->{labels_tags}),
			countries_tags => _snapshot_array($product_ref->{countries_tags}),
			food_groups_tags => _snapshot_array($product_ref->{food_groups_tags}),
		},
		flags => {
			is_european_product => $helper_state_ref->{is_european_product},
			has_animal_origin_category => $helper_state_ref->{has_animal_origin_category},
			ignore_energy_calculated_error => $helper_state_ref->{ignore_energy_calculated_error},
		},
		category_props => {
			minimum_number_of_ingredients => _snapshot_number($helper_state_ref->{minimum_number_of_ingredients}),
		},
		nutrition => _snapshot_hash($product_ref->{nutrition}),
	};
}

sub _legacy_check_tags_for {
	my ($product_ref) = @_;

	return {
		bug => _snapshot_array($product_ref->{data_quality_bugs_tags}),
		info => _snapshot_array($product_ref->{data_quality_info_tags}),
		completeness => _snapshot_array($product_ref->{data_quality_completeness_tags}),
		warning => _snapshot_array($product_ref->{data_quality_warnings_tags}),
		error => _snapshot_array($product_ref->{data_quality_errors_tags}),
	};
}

while (my $line = <$input_fh>) {
	chomp $line;
	next if $line !~ /\S/;

	my $product_ref = $json->decode($line);

	if (not defined $product_ref->{product_type}) {
		$product_ref->{product_type} = $options{product_type};
	}
	normalize_product_data($product_ref);
	analyze_and_enrich_product_data($product_ref, {});

	my ($minimum_number_of_ingredients, $minimum_number_category_id)
		= get_inherited_property_from_categories_tags($product_ref, "minimum_number_of_ingredients:en");
	my ($ignore_energy_calculated_error, $ignore_energy_category_id)
		= get_inherited_property_from_categories_tags($product_ref, "ignore_energy_calculated_error:en");
	my $animal_origin_categories = get_all_tags_having_property($product_ref, "categories", "food_of_animal_origin:en");
	my $helper_state = {
		is_european_product => is_european_product($product_ref) ? JSON::PP::true : JSON::PP::false,
		has_animal_origin_category => (scalar keys %{$animal_origin_categories}) > 0 ? JSON::PP::true : JSON::PP::false,
		minimum_number_of_ingredients => $minimum_number_of_ingredients,
		ignore_energy_calculated_error => ($ignore_energy_calculated_error and $ignore_energy_calculated_error eq "yes")
			? JSON::PP::true
			: JSON::PP::false,
	};

	print {$output_fh} $json->encode(
		{
			code => $product_ref->{code},
			enriched_snapshot => _enriched_snapshot_for($product_ref, $helper_state),
			legacy_check_tags => _legacy_check_tags_for($product_ref),
		}
	) . "\n";
}

close $input_fh;
close $output_fh;
