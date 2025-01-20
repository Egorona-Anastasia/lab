import threading
import queue
import os
from PIL import Image

# Create blocking queues
data_queue = queue.Queue(maxsize=10)  # Queue for pixels
result_queue = queue.Queue(maxsize=10)  # Queue for results

# Function to invert color
def invert_color(pixel):
    return (255 - pixel[0], 255 - pixel[1], 255 - pixel[2])

# Producer: generates pixels
def producer(image):
    print("Producer: Starting image processing...")
    pixel_count = 0
    for y in range(image.height):
        for x in range(image.width):
            pixel = image.getpixel((x, y))
            data_queue.put((x, y, pixel))  # Send coordinates and pixel
            pixel_count += 1
    # Indicate that no more data will be sent
    for _ in range(4):  # Send 4 termination signals for 4 Consumers
        data_queue.put(None)
    print(f"Producer: Image processing completed. Processed {pixel_count} pixels.")

# Consumer: processes pixels
def consumer(consumer_id):
    print(f"Consumer {consumer_id}: Starting pixel processing...")
    processed_count = 0
    while True:
        item = data_queue.get()
        if item is None:  # Check for termination
            print(f"Consumer {consumer_id}: Terminating.")
            break
        x, y, pixel = item  # Get coordinates and pixel
        inverted_pixel = invert_color(pixel)
        result_queue.put((x, y, inverted_pixel))  # Send coordinates and inverted pixel
        processed_count += 1
    print(f"Consumer {consumer_id}: Pixel processing completed. Processed {processed_count} pixels.")

# Result Consumer: collects results
def result_consumer(image):
    print("Result Consumer: Starting result collection...")
    collected_count = 0
    while True:
        item = result_queue.get()
        if item is None:  # Check for termination
            break
        x, y, inverted_pixel = item
        image.putpixel((x, y), inverted_pixel)  # Apply inverted pixel back to the image
        collected_count += 1
    print(f"Result Consumer: Result collection completed. Collected {collected_count} pixels.")

# Main function
def process_image(image_path):
    # Load the image
    try:
        image = Image.open(image_path)
        image = image.convert("RGB")
    except FileNotFoundError:
        print(f"Error: File '{image_path}' not found.")
        return

    # Create and start threads
    producer_thread = threading.Thread(target=producer, args=(image,))
    consumer_threads = [threading.Thread(target=consumer, args=(i,)) for i in range(4)]  # 4 threads for processing
    result_thread = threading.Thread(target=result_consumer, args=(image,))

    producer_thread.start()
    for thread in consumer_threads:
        thread.start()
    result_thread.start()

    # Wait for threads to finish
    producer_thread.join()
    for thread in consumer_threads:
        thread.join()

    # Terminate Result Consumer
    for _ in range(1):  # Send one termination signal for Result Consumer
        result_queue.put(None)
    result_thread.join()
    
    # Save the processed image
    output_path = f"output_{os.path.basename(image_path)}"
    image.save(output_path)
    print(f"Processing completed. Result saved as '{output_path}'.")

def main():
    while True:
        directory = input("Enter the path to the directory with images: ")

        if os.path.isdir(directory):
            print(f"Directory '{directory}' found.")
            break
        else:
            print(f"Error: Directory '{directory}' not found. Please try again.")

    for filename in os.listdir(directory):
        if filename.endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            process_image(os.path.join(directory, filename))


if __name__ == "__main__":
    main()

