"""
Course: CSE 351
Assignment: 06
Author: [Your Name]

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

# Folders
INPUT_FOLDER = "faces"
STEP1_OUTPUT_FOLDER = "step1_smoothed"
STEP2_OUTPUT_FOLDER = "step2_grayscale"
STEP3_OUTPUT_FOLDER = "step3_edges"

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']


# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")


# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image  # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)


# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)


# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1:  # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image  # Or raise error
    return cv2.Canny(image, threshold1, threshold2)


# ---------------------------------------------------------------------------
def process_images_in_folder(input_folder,              # input folder with images
                             output_folder,             # output folder for processed images
                             processing_function,       # function to process the image (ie., task_...())
                             load_args=None,            # Optional args for cv2.imread
                             processing_args=None):     # Optional args for processing function

    create_folder_if_not_exists(output_folder)
    print(f"\nProcessing images from '{input_folder}' to '{output_folder}'...")

    processed_count = 0
    for filename in os.listdir(input_folder):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue

        input_image_path = os.path.join(input_folder, filename)
        output_image_path = os.path.join(output_folder, filename)  # Keep original filename

        try:
            # Read the image
            if load_args is not None:
                img = cv2.imread(input_image_path, load_args)
            else:
                img = cv2.imread(input_image_path)

            if img is None:
                print(f"Warning: Could not read image '{input_image_path}'. Skipping.")
                continue

            # Apply the processing function
            if processing_args:
                processed_img = processing_function(img, *processing_args)
            else:
                processed_img = processing_function(img)

            # Save the processed image
            cv2.imwrite(output_image_path, processed_img)

            processed_count += 1
        except Exception as e:
            print(f"Error processing file '{input_image_path}': {e}")

    print(f"Finished processing. {processed_count} images processed into '{output_folder}'.")


# ---------------------------------------------------------------------------
# Worker for smoothing stage
def smooth_worker(q_in, q_out):
    while True:
        filename = q_in.get()
        if filename is None:
            # Signal next stage that no more images will come
            break

        input_path = os.path.join(INPUT_FOLDER, filename)
        img = cv2.imread(input_path)
        if img is None:
            continue

        smoothed = task_smooth_image(img, GAUSSIAN_BLUR_KERNEL_SIZE)
        output_path = os.path.join(STEP1_OUTPUT_FOLDER, filename)
        cv2.imwrite(output_path, smoothed)

        # Pass filename to next stage
        q_out.put(filename)
    return


# ---------------------------------------------------------------------------
# Worker for grayscale conversion stage
def grayscale_worker(q_in, q_out):
    while True:
        filename = q_in.get()
        if filename is None:
            # Once sentinel received, propagate to next stage and exit
            q_out.put(None)
            break

        input_path = os.path.join(STEP1_OUTPUT_FOLDER, filename)
        img = cv2.imread(input_path)
        if img is None:
            continue

        gray = task_convert_to_grayscale(img)
        output_path = os.path.join(STEP2_OUTPUT_FOLDER, filename)
        cv2.imwrite(output_path, gray)

        # Pass filename to next stage
        q_out.put(filename)


# ---------------------------------------------------------------------------
# Worker for edge detection stage
def edges_worker(q_in):
    while True:
        filename = q_in.get()
        if filename is None:
            # No more images; exit
            break

        input_path = os.path.join(STEP2_OUTPUT_FOLDER, filename)
        img = cv2.imread(input_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            continue

        edges = task_detect_edges(img, CANNY_THRESHOLD1, CANNY_THRESHOLD2)
        output_path = os.path.join(STEP3_OUTPUT_FOLDER, filename)
        cv2.imwrite(output_path, edges)


# ---------------------------------------------------------------------------
def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    # TODO
    # - create queues
    # - create barriers
    # - create the three processes groups
    # - you are free to change anything in the program as long as you
    #   do all requirements.

    # Create output folders if they don't exist
    create_folder_if_not_exists(STEP1_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP2_OUTPUT_FOLDER)
    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)

    # Decide on number of worker processes for each stage
    num_cpu = mp.cpu_count()
    num_smoothers = num_cpu
    num_grays = num_cpu
    num_edges = num_cpu

    # Create queues for inter-process communication
    q1 = mp.Queue(maxsize=num_cpu * 2)
    q2 = mp.Queue(maxsize=num_cpu * 2)
    q3 = mp.Queue(maxsize=num_cpu * 2)

    # Start smoother processes
    smoothers = []
    for _ in range(num_smoothers):
        p = mp.Process(target=smooth_worker, args=(q1, q2))
        p.start()
        smoothers.append(p)

    # Start grayscale converter processes
    grays = []
    for _ in range(num_grays):
        p = mp.Process(target=grayscale_worker, args=(q2, q3))
        p.start()
        grays.append(p)

    # Start edge detector processes
    edgers = []
    for _ in range(num_edges):
        p = mp.Process(target=edges_worker, args=(q3,))
        p.start()
        edgers.append(p)

    # Enqueue all filenames into q1
    for filename in os.listdir(INPUT_FOLDER):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue
        q1.put(filename)

    # Send sentinel values to smoothing stage (one per smoother)
    for _ in range(num_smoothers):
        q1.put(None)

    # Wait for all smoothers to finish
    for p in smoothers:
        p.join()

    # At this point, all smoothing is done and q2 has received all real filenames
    # Now send sentinel values to grayscale stage (one per grayscale worker)
    for _ in range(num_grays):
        q2.put(None)

    # Wait for all grayscale converters to finish
    for p in grays:
        p.join()

    # At this point, all grayscale conversion is done and q3 has received all real filenames
    # Now send sentinel values to edge detection stage (one per edge worker)
    for _ in range(num_edges):
        q3.put(None)

    # Wait for all edge detectors to finish
    for p in edgers:
        p.join()

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Grayscale images are in: '{STEP1_OUTPUT_FOLDER}'")
    print(f"Smoothed images are in: '{STEP2_OUTPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    # check for input folder
    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')
